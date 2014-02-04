package reactor.graph;

import reactor.function.Predicate;

/**
 * @author Jon Brisbin
 */
public abstract class Predicates {

	protected Predicates() {
	}

	public static <T> Predicate<T> isSpecificType(final Class<? extends T> type) {
		return new Predicate<T>() {
			@Override
			public boolean test(T obj) {
				if(null == obj) {
					return false;
				}
				return obj.getClass() == type;
			}
		};
	}

	public static <T> Predicate<T> isAssignableFrom(final Class<? extends T> type) {
		return new Predicate<T>() {
			@Override
			public boolean test(T obj) {
				if(null == obj) {
					return false;
				}
				return type.isAssignableFrom(obj.getClass());
			}
		};
	}

}
