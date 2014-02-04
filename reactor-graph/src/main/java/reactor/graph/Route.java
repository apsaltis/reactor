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

	private final Node<?>    node;
	private final Observable observable;
	private final Route<?>   parent;

	Route(Node<?> node, Observable observable, Route<?> parent) {
		this.node = node;
		this.observable = observable;
		this.parent = (null != parent ? parent : this);
	}

	public Route<T> routeTo(String nodeName) {
		final Node<?> routeToNode = node.getGraph().getNode(nodeName);
		observable.on(onValue, new Consumer<Event<?>>() {
			@Override
			public void accept(Event ev) {
				routeToNode.notifyValue(ev);
			}
		});
		return this;
	}

	public <V> Route<V> then(final Function<T, V> fn) {
		final Route<V> route = new Route<V>(node, observable, parent);
		observable.on(onValue, new Consumer<Event<T>>() {
			@SuppressWarnings("unchecked")
			@Override
			public void accept(Event<T> ev) {
				V obj = fn.apply(ev.getData());
				Event<V> newEv = node.getGraph().getEventFactory().get().setData(obj);
				route.notifyValue(newEv);
			}
		});
		return route;
	}

	public Route<T> otherwise() {
		final Route<T> route = new Route<T>(node, observable, parent);
		observable.on(parent.onOtherwise, new Consumer<Event<T>>() {
			@Override
			public void accept(Event<T> ev) {
				route.notifyValue(ev);
			}
		});
		return route;
	}

	public Route<T> end(final Consumer<T> consumer) {
		observable.on(onValue, new Consumer<Event<T>>() {
			@Override
			public void accept(Event<T> ev) {
				consumer.accept(ev.getData());
			}
		});
		return this;
	}

	@SuppressWarnings("unchecked")
	Node<T> getNode() {
		return (Node<T>)node;
	}

	void notifyValue(Event<T> ev) {
		observable.notify(onValue.getObject(), ev);
	}

	void notifyOtherwise(Event<T> ev) {
		observable.notify(onOtherwise.getObject(), ev);
	}

}
