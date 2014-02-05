package reactor.graph;

import reactor.core.Observable;
import reactor.event.Event;
import reactor.event.selector.Selector;
import reactor.event.selector.Selectors;
import reactor.function.Consumer;
import reactor.function.Function;

/**
 * A {@literal Route} represents a connection between two {@literal Nodes}.
 *
 * @author Jon Brisbin
 */
public class Route<T> {

	private final Selector onValue     = Selectors.anonymous();
	private final Selector onOtherwise = Selectors.anonymous();
	private final Selector onError     = Selectors.anonymous();

	private final Node<?>    node;
	private final Observable observable;

	Route(Node<?> node, Observable observable) {
		this.node = node;
		this.observable = observable;
	}

	/**
	 * Route value events coming into this {@literal Route} to the named {@literal Node}, which must already exist in the
	 * {@literal Graph}.
	 *
	 * @param nodeName
	 * 		name of the {@literal Node} to forward events to
	 *
	 * @return {@literal this}
	 */
	@SuppressWarnings("unchecked")
	public Route<T> routeTo(String nodeName) {
		final Node<T> routeToNode = (Node<T>)node.getGraph().getNode(nodeName);
		observable.on(onValue, new Consumer<Event>() {
			@SuppressWarnings("unchecked")
			@Override
			public void accept(Event ev) {
				routeToNode.notifyValue(ev);
			}
		});
		observable.on(onError, new Consumer<Event>() {
			@SuppressWarnings("unchecked")
			@Override
			public void accept(Event ev) {
				routeToNode.notifyError(ev);
			}
		});
		return this;
	}

	/**
	 * Consume events passing through this {@literal Route}.
	 *
	 * @param consumer
	 * 		the {@literal Consumer} which will consume events
	 *
	 * @return {@literal this}
	 */
	public Route<T> consume(final Consumer<T> consumer) {
		observable.on(onValue, new Consumer<Event<T>>() {
			@Override
			public void accept(Event<T> ev) {
				try {
					consumer.accept(ev.getData());
				} catch(Throwable t) {
					node.notifyError(ev.copy(t));
				}
			}
		});
		return this;
	}

	/**
	 * Transform events coming into this {@literal Route} by applying the given {@link reactor.function.Function}.
	 *
	 * @param fn
	 * 		the transformation {@link reactor.function.Function}
	 * @param <V>
	 * 		the type of the returned event
	 *
	 * @return the new {@literal Route}
	 */
	@SuppressWarnings("unchecked")
	public <V> Route<V> then(final Function<T, V> fn) {
		final Route<V> newRoute = node.createRoute();
		observable.on(onValue, new Consumer<Event<T>>() {
			@Override
			public void accept(Event<T> ev) {
				try {
					V obj = fn.apply(ev.getData());
					newRoute.notifyValue(ev.copy(obj));
				} catch(Throwable t) {
					newRoute.notifyError(ev.copy(t));
				}
			}
		});
		return newRoute;
	}

	/**
	 * Capture events coming into this {@literal Route} that have failed the {@link reactor.function.Predicate} test
	 * which created this {@literal Route}.
	 *
	 * @return the new {@literal Route}
	 */
	public Route<T> otherwise() {
		final Route<T> newRoute = node.createRoute();
		observable.on(onOtherwise, new Consumer<Event<T>>() {
			@Override
			public void accept(Event<T> ev) {
				newRoute.notifyValue(ev);
			}
		});
		return newRoute;
	}

	/**
	 * End this {@literal Route} and go back to the {@literal Node} from which this {@literal Route} was created.
	 *
	 * @return the {@literal Node} from which this {@literal Route} was created
	 */
	@SuppressWarnings("unchecked")
	private Node<T> end() {
		return (Node<T>)node;
	}

	void notifyValue(Event<T> ev) {
		observable.notify(onValue.getObject(), ev);
	}

	void notifyOtherwise(Event<T> ev) {
		observable.notify(onOtherwise.getObject(), ev);
	}

	void notifyError(Event<Throwable> ev) {
		observable.notify(onError.getObject(), ev);
	}

}
