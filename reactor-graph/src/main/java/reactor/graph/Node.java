package reactor.graph;

import reactor.core.Observable;
import reactor.event.Event;
import reactor.event.selector.Selector;
import reactor.event.selector.Selectors;
import reactor.function.Consumer;
import reactor.function.Function;
import reactor.function.Predicate;
import reactor.util.UUIDUtils;

/**
 * A {@literal Node} represents an action or a point at which events can be routed to other {@literal Node Nodes} based
 * on different criteria.
 *
 * @author Jon Brisbin
 */
public class Node<T> {

	private final Selector onValue = Selectors.anonymous();
	private final Selector onError = Selectors.anonymous();

	private final String     name;
	private final Graph<?>   graph;
	private final Observable observable;
	private final Node<?>    parent;

	Node(Graph<?> graph, Observable observable, Node<?> parent) {
		this(UUIDUtils.create().toString(), graph, observable, parent);
	}

	Node(String name, Graph<?> graph, Observable observable, Node<?> parent) {
		this.name = name;
		this.graph = graph;
		this.observable = observable;
		this.parent = parent;
	}

	/**
	 * Get the name of this {@literal Node}.
	 *
	 * @return this Node's name
	 */
	public String getName() {
		return name;
	}

	public <X extends Throwable> Node<X> when(final Class<X> errorType) {
		final Node<X> newNode = createChild();
		consumeError(new Consumer<Event<Throwable>>() {
			@SuppressWarnings("unchecked")
			@Override
			public void accept(Event ev) {
				if(errorType.isInstance(ev.getData())) {
					newNode.notifyValue(ev);
				}
			}
		});
		return newNode;
	}

	/**
	 * Create a {@link Route} that will publish values that pass the given {@link reactor.function.Predicate} test and
	 * publish values that fail the test into the Route's {@link reactor.graph.Route#otherwise()} {@literal Route}.
	 *
	 * @param predicate
	 * 		the {@link reactor.function.Predicate} test
	 *
	 * @return a new {@link reactor.graph.Route}
	 */
	public Route<T> when(final Predicate<T> predicate) {
		final Route<T> route = createRoute();
		consumeValue(new Consumer<Event<T>>() {
			@Override
			public void accept(Event<T> ev) {
				if(predicate.test(ev.getData())) {
					route.notifyValue(ev);
				} else {
					route.notifyOtherwise(ev);
				}
			}
		});
		return route;
	}

	/**
	 * Transform the values coming into this {@literal Node} by applying the given {@link reactor.function.Function}.
	 *
	 * @param fn
	 * 		the transformation {@link reactor.function.Function}
	 * @param <V>
	 * 		the type of the returned value
	 *
	 * @return a new {@literal Node}
	 */
	public <V> Node<V> then(final Function<T, V> fn) {
		final Node<V> newNode = createChild();
		consumeValue(new Consumer<Event<T>>() {
			@SuppressWarnings("unchecked")
			@Override
			public void accept(Event<T> ev) {
				try {
					V obj = fn.apply(ev.getData());
					newNode.notifyValue(ev.copy(obj));
				} catch(Throwable t) {
					newNode.notifyError(ev.copy(t));
				}
			}
		});
		return newNode;
	}

	/**
	 * Consume values coming into this {@literal Node}.
	 *
	 * @param consumer
	 * 		the {@link reactor.function.Consumer} that will consume values
	 *
	 * @return {@literal this}
	 */
	public Node<T> consume(final Consumer<T> consumer) {
		consumeValue(new Consumer<Event<T>>() {
			@SuppressWarnings("unchecked")
			@Override
			public void accept(Event<T> ev) {
				try {
					consumer.accept(ev.getData());
				} catch(Throwable t) {
					Event<Throwable> evx = graph.getEventFactory().get().setData(t);
					notifyError(evx);
				}
			}
		});
		return this;
	}

	Graph<?> getGraph() {
		return graph;
	}

	void notifyValue(Event<T> ev) {
		observable.notify(onValue.getObject(), ev);
	}

	void notifyError(Event<Throwable> ev) {
		observable.notify(onError.getObject(), ev);
	}

	void consumeValue(Consumer<Event<T>> consumer) {
		observable.on(onValue, consumer);
	}

	void consumeError(Consumer<Event<Throwable>> consumer) {
		observable.on(onError, consumer);
	}

	<V> Node<V> createChild() {
		return new Node<V>(graph, observable, this);
	}

	<V> Route<V> createRoute() {
		return new Route<V>(this, observable);
	}

	@Override
	public String toString() {
		return "Node{" +
				"name='" + name + '\'' +
				'}';
	}

}
