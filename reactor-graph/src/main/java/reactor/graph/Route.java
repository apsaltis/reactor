package reactor.graph;

import reactor.core.Observable;
import reactor.event.Event;
import reactor.event.selector.Selector;
import reactor.event.selector.Selectors;
import reactor.event.support.EventConsumer;
import reactor.function.Consumer;
import reactor.function.Function;

/**
 * @author Jon Brisbin
 */
public class Route<T> {

	private final Selector onValue     = Selectors.anonymous();
	private final Selector onError     = Selectors.anonymous();
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
			@SuppressWarnings("unchecked")
			@Override
			public void accept(Event ev) {
				routeToNode.notifyValue(ev);
			}
		});
		return this;
	}

	public <X extends Throwable> Route<X> when(final Class<X> errorType) {
		final Route<X> route = new Route<X>(node, observable, this);
		observable.on(onError, new Consumer<Event<Throwable>>() {
			@SuppressWarnings("unchecked")
			@Override
			public void accept(Event<Throwable> ev) {
				if(errorType.isInstance(ev.getData())) {
					route.notifyValue((Event<X>)ev);
				}
			}
		});
		return route;
	}

	public Route<T> then(Consumer<T> consumer) {
		observable.on(onValue, new EventConsumer<T>(consumer));
		return this;
	}

	public <V> Route<V> then(final Function<T, V> fn) {
		final Route<V> route = new Route<V>(node, observable, this);
		observable.on(onValue, new Consumer<Event<T>>() {
			@SuppressWarnings("unchecked")
			@Override
			public void accept(Event<T> ev) {
				try {
					V obj = fn.apply(ev.getData());
					Event<V> newEv = node.getGraph().getEventFactory().get().setData(obj);
					route.notifyValue(newEv);
				} catch(Throwable t) {
					Event<Throwable> evx = node.getGraph().getEventFactory().get().setData(t);
					observable.notify(onError, evx);
				}
			}
		});
		return route;
	}

	public Route<T> otherwise() {
		final Route<T> route = new Route<T>(node, observable, this);
		observable.on(parent.onOtherwise, new Consumer<Event<T>>() {
			@Override
			public void accept(Event<T> ev) {
				route.notifyValue(ev);
			}
		});
		return route;
	}

	@SuppressWarnings("unchecked")
	public <V> Route<V> end(final Consumer<T> consumer) {
		observable.on(onValue, new Consumer<Event<T>>() {
			@SuppressWarnings("unchecked")
			@Override
			public void accept(Event<T> ev) {
				try {
					consumer.accept(ev.getData());
				} catch(Throwable t) {
					Event<Throwable> evx = node.getGraph().getEventFactory().get().setData(t);
					observable.notify(onError, evx);
				}
			}
		});
		return (Route<V>)parent;
	}

	void notifyValue(Event<T> ev) {
		observable.notify(onValue.getObject(), ev);
	}

	void notifyOtherwise(Event<T> ev) {
		observable.notify(onOtherwise.getObject(), ev);
	}

}
