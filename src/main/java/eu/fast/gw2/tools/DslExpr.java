package eu.fast.gw2.tools;

import java.util.ArrayList;
import java.util.List;

final class DslExpr {

    static DslExpr parse(String src) {
        return new Parser(src).parse();
    }

    double eval(OverlayDslEngine.DslContext ctx) {
        return root.eval(ctx);
    }

    private final Node root;

    private DslExpr(Node root) {
        this.root = root;
    }

    private interface Node {
        double eval(OverlayDslEngine.DslContext ctx);
    }

    private static final class Num implements Node {
        final double v;

        Num(double v) {
            this.v = v;
        }

        public double eval(OverlayDslEngine.DslContext ctx) {
            return v;
        }
    }

    private static final class Var implements Node {
        final String name;

        Var(String n) {
            this.name = n;
        }

        public double eval(OverlayDslEngine.DslContext ctx) {
            double v = ctx.var(name);
            return Double.isNaN(v) ? 0.0 : v;
        }
    }

    private static final class Str implements Node {
        final String v;

        Str(String v) {
            this.v = v;
        }

        public double eval(OverlayDslEngine.DslContext ctx) {
            return 0.0;
        }

        String value() {
            return v;
        }
    }

    private static final class Bin implements Node {
        final char op;
        final Node a, b;

        Bin(char op, Node a, Node b) {
            this.op = op;
            this.a = a;
            this.b = b;
        }

        public double eval(OverlayDslEngine.DslContext ctx) {
            double x = a.eval(ctx), y = b.eval(ctx);
            return switch (op) {
                case '+' -> x + y;
                case '-' -> x - y;
                case '*' -> x * y;
                case '/' -> (y == 0.0 ? 0.0 : x / y);
                default -> 0.0;
            };
        }
    }

    private static final class Call implements Node {
        final String fn;
        final List<Node> args;

        Call(String fn, List<Node> args) {
            this.fn = fn;
            this.args = args;
        }

        public double eval(OverlayDslEngine.DslContext ctx) {
            return switch (fn) {
                case "BUY" -> ctx.BUY(argNum(0, ctx));
                case "SELL" -> ctx.SELL(argNum(0, ctx));
                case "VENDOR" -> ctx.VENDOR(argNum(0, ctx));
                case "NET" -> ctx.NET(argNum(0, ctx), argNum(1, ctx));
                case "QTY" -> ctx.QTY();
                case "FALLBACK" -> {
                    double[] xs = new double[args.size()];
                    for (int i = 0; i < args.size(); i++)
                        xs[i] = args.get(i).eval(ctx);
                    yield ctx.FALLBACK(xs);
                }
                case "FLOOR" -> ctx.FLOOR(argNum(0, ctx));
                case "EV" -> 0.0; // used via .buy/.sell
                default -> 0.0;
            };
        }

        double argNum(int i, OverlayDslEngine.DslContext ctx) {
            return (i >= args.size()) ? 0.0 : args.get(i).eval(ctx);
        }

        Node getArg(int i) {
            return (i < args.size() ? args.get(i) : null);
        }
    }

    private static final class Prop implements Node {
        final Call target;
        final String name;

        Prop(Call target, String name) {
            this.target = target;
            this.name = name;
        }

        public double eval(OverlayDslEngine.DslContext ctx) {
            Node a0 = target.getArg(0); // key
            Node a1 = target.getArg(1); // taxes
            String key = null;
            if (a0 instanceof Str s)
                key = s.value();
            else if (a0 instanceof Var v)
                key = ctx.strVar(v.name);
            if (key == null)
                return 0.0;
            double taxes = (a1 == null ? 0.0 : a1.eval(ctx));
            var r = ctx.EV(key, taxes);
            return "buy".equalsIgnoreCase(name) ? r.buy : r.sell;
        }
    }

    private static final class Parser {
        private final String s;
        private int i;

        Parser(String s) {
            this.s = s;
        }

        DslExpr parse() {
            skip();
            Node n = expr();
            skip();
            if (i != s.length())
                throw err("Unexpected trailing input");
            return new DslExpr(n);
        }

        private Node expr() {
            Node n = term();
            while (true) {
                skip();
                if (peek('+') || peek('-')) {
                    char op = s.charAt(i++);
                    Node r = term();
                    n = new Bin(op, n, r);
                } else
                    break;
            }
            return n;
        }

        private Node term() {
            Node n = factor();
            while (true) {
                skip();
                if (peek('*') || peek('/')) {
                    char op = s.charAt(i++);
                    Node r = factor();
                    n = new Bin(op, n, r);
                } else
                    break;
            }
            return n;
        }

        // factor := NUMBER | STRING | IDENT | IDENT '(' args ')' [ '.' IDENT ] | '('
        // expr ')'
        private Node factor() {
            skip();
            if (peek('(')) {
                i++;
                Node n = expr();
                expect(')');
                return n;
            }
            if (peek('"') || peek('\'')) {
                char q = s.charAt(i++);
                int start = i;
                while (i < s.length() && s.charAt(i) != q)
                    i++;
                if (i >= s.length())
                    throw err("Unterminated string");
                String lit = s.substring(start, i);
                i++;
                return new Str(lit);
            }
            if (isDigit(peekc()) || (peek('-') && isDigit(peekc(1))))
                return number();
            if (isIdentStart(peekc())) {
                String ident = ident();
                skip();
                if (peek('(')) {
                    i++;
                    List<Node> args = new ArrayList<>();
                    skip();
                    if (!peek(')')) {
                        do {
                            args.add(expr());
                            skip();
                        } while (consume(','));
                    }
                    expect(')');
                    Call call = new Call(ident, args);
                    skip();
                    if (peek('.')) {
                        i++;
                        String prop = ident();
                        if (!"EV".equalsIgnoreCase(ident))
                            throw err("Property access only supported on EV(...).");
                        if (!("buy".equalsIgnoreCase(prop) || "sell".equalsIgnoreCase(prop)))
                            throw err("Only .buy or .sell supported");
                        return new Prop(call, prop);
                    }
                    return call;
                } else {
                    return new Var(ident);
                }
            }
            throw err("Unexpected token");
        }

        private Node number() {
            int start = i;
            if (peek('-'))
                i++;
            while (isDigit(peekc()))
                i++;
            if (peek('.')) {
                i++;
                while (isDigit(peekc()))
                    i++;
            }
            double v = Double.parseDouble(s.substring(start, i));
            return new Num(v);
        }

        private String ident() {
            int start = i;
            if (!isIdentStart(peekc()))
                throw err("Identifier expected");
            i++;
            while (isIdentPart(peekc()))
                i++;
            return s.substring(start, i);
        }

        private void expect(char c) {
            skip();
            if (!peek(c))
                throw err("Expected '" + c + "'");
            i++;
        }

        private boolean consume(char c) {
            skip();
            if (peek(c)) {
                i++;
                return true;
            }
            return false;
        }

        private void skip() {
            while (i < s.length() && " \t\r\n".indexOf(s.charAt(i)) >= 0)
                i++;
        }

        private boolean peek(char c) {
            return (i < s.length() && s.charAt(i) == c);
        }

        private char peekc() {
            return (i < s.length() ? s.charAt(i) : '\0');
        }

        private char peekc(int off) {
            int j = i + off;
            return (j < s.length() ? s.charAt(j) : '\0');
        }

        private static boolean isDigit(char c) {
            return c >= '0' && c <= '9';
        }

        private static boolean isIdentStart(char c) {
            return c == '_' || Character.isLetter(c);
        }

        private static boolean isIdentPart(char c) {
            return isIdentStart(c) || isDigit(c);
        }

        private RuntimeException err(String msg) {
            return new RuntimeException(msg + " at col " + (i + 1));
        }
    }
}
