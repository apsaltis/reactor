package reactor.graph;

import reactor.core.Observable;
import reactor.event.Event;
import reactor.event.selector.Selector;
import reactor.event.selector.Selectors;
import reactor.function.Consumer;
import reactor.function.Function;

/**
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
