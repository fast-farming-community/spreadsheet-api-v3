package eu.fast.gw2.tools;

import java.util.function.Function;

import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityTransaction;

public class Jpa {

    @FunctionalInterface
    public interface TxVoid {
        void run(EntityManager em);
    }

    public static <R> R tx(Function<EntityManager, R> f) {
        var emf = HibernateUtil.emf();
        try (var em = emf.createEntityManager()) {
            EntityTransaction tx = em.getTransaction();
            tx.begin();
            try {
                R r = f.apply(em);
                tx.commit();
                return r;
            } catch (RuntimeException e) {
                if (tx.isActive())
                    tx.rollback();
                throw e;
            }
        }
    }

    public static void txVoid(TxVoid f) {
        var emf = HibernateUtil.emf();
        try (var em = emf.createEntityManager()) {
            EntityTransaction tx = em.getTransaction();
            tx.begin();
            try {
                f.run(em);
                tx.commit();
            } catch (RuntimeException e) {
                if (tx.isActive())
                    tx.rollback();
                throw e;
            }
        }
    }
}
