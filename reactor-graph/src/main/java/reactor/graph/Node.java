package reactor.graph;

import reactor.core.Observable;
import reactor.event.Event;
import reactor.event.support.EventConsumer;
import reactor.function.Consumer;
import reactor.function.Predicate;

/**
 * @author Jon Brisbin
 */
public class Node<T> {

	private final String     name;
	private final Graph<T>   graph;
	private final Observable observable;

	Node(String name, Graph<T> graph, Observable observable) {
		this.name = name;
		this.graph = graph;
		this.observable = observable;
	}

	public Route<T> when(final Predicate<T> predicate) {
		final Route<T> route = new Route<T>(this, observable, null);
		observable.on(new Consumer<Event<T>>() {
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

	public Node<T> then(final Consumer<T> consumer) {
		observable.on(new EventConsumer<T>(consumer));
		return this;
	}

	Graph<T> getGraph() {
		return graph;
	}

	void notifyValue(Event<T> ev) {
		observable.notify(ev);
	}

	@Override
	public String toString() {
		return "Node{" +
				"name='" + name + '\'' +
				'}';
	}

}
