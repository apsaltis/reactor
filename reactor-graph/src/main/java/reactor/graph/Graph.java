package reactor.graph;

import reactor.core.Environment;
import reactor.core.Reactor;
import reactor.core.alloc.factory.BatchFactorySupplier;
import reactor.core.spec.Reactors;
import reactor.event.Event;
import reactor.event.dispatch.Dispatcher;
import reactor.function.Consumer;
import reactor.function.Supplier;
import reactor.util.Assert;
import reactor.util.UUIDUtils;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A {@code Graph} is a directed set of actions based on the
 * <a href="http://en.wikipedia.org/wiki/Directed_acyclic_graph">directed acyclic graph concept</a>.
 * <p>
 * A {@literal Graph} contains a set of {@link Node Nodes} linked together by {@link Route Routes}. Routing is
 * controlled by assigning a {@link reactor.function.Predicate} which will notify the {@literal Node} of new values.
 * Values that don't pass the {@literal Predicate} can be accessed from calling {@link reactor.graph.Route#otherwise()}
 * and assigning actions to that {@literal Route}.
 * </p>
 * <p>
 * {@literal Nodes} can be loosely-linked by creating them ahead of time and using the {@link Route#routeTo(String)}
 * method to send an event to a specific, named {@literal Node}.
 * </p>
 *
 * @author Jon Brisbin
 */
public class Graph<T> implements Consumer<T> {

	private final Map<String, Node<T>> nodes = new ConcurrentHashMap<String, Node<T>>();

	private final Environment                 env;
	private final Dispatcher                  defaultDispatcher;
	private final BatchFactorySupplier<Event> eventFactory;
	private       Node<T>                     startNode;

	private Graph(Environment env, Dispatcher defaultDispatcher) {
		this.env = env;
		this.defaultDispatcher = defaultDispatcher;
		this.eventFactory = new BatchFactorySupplier<Event>(
				1024,
				new Supplier<Event>() {
					@SuppressWarnings("unchecked")
					@Override
					public Event get() {
						return new Event(null);
					}
				}
		);
	}

	/**
	 * Create a {@literal Graph} with the given {@link reactor.core.Environment}.
	 *
	 * @param env
	 * 		the {@link reactor.core.Environment} to use
	 * @param <T>
	 * 		the type of data coming into this {@literal Graph}
	 *
	 * @return the new {@literal Graph}
	 */
	public static <T> Graph<T> create(Environment env) {
		return new Graph<T>(env, env.getDefaultDispatcher());
	}

	/**
	 * Create a {@literal Graph} with the given {@link reactor.core.Environment} and assigning the given {@literal
	 * Dispatcher} as a default if no other {@literal Dispatcher} is specified at {@literal Node} creation.
	 *
	 * @param env
	 * 		the {@link reactor.core.Environment} to use
	 * @param dispatcher
	 * 		the {@link reactor.event.dispatch.Dispatcher} to use
	 * @param <T>
	 * 		the type of data coming into this {@literal Graph}
	 *
	 * @return the new {@literal Graph}
	 */
	public static <T> Graph<T> create(Environment env, String dispatcher) {
		return new Graph<T>(env, env.getDispatcher(dispatcher));
	}

	/**
	 * Create a {@literal Graph} with the given {@link reactor.core.Environment} and assigning the given {@literal
	 * Dispatcher} as a default if no other {@literal Dispatcher} is specified at {@literal Node} creation.
	 *
	 * @param env
	 * 		the {@link reactor.core.Environment} to use
	 * @param dispatcher
	 * 		the {@link reactor.event.dispatch.Dispatcher} to use
	 * @param <T>
	 * 		the type of data coming into this {@literal Graph}
	 *
	 * @return the new {@literal Graph}
	 */
	public static <T> Graph<T> create(Environment env, Dispatcher dispatcher) {
		return new Graph<T>(env, dispatcher);
	}

	/**
	 * Use the named {@literal Node} as the initial, starting {@literal Node} when new data comes into the {@literal
	 * Graph}.
	 *
	 * @param name
	 * 		the {@literal Node} to use as the {@literal Node} which receives incoming data
	 *
	 * @return {@literal this}
	 */
	public Graph<T> startNode(String name) {
		Node<T> node = getNode(name);
		this.startNode = node;
		return this;
	}

	/**
	 * Create a new {@literal Node} in this {@literal Graph} with a generated, UUID name.
	 *
	 * @return the new {@literal Node}
	 */
	public Node<T> node() {
		return node(UUIDUtils.create().toString(), null);
	}

	/**
	 * Create a new {@literal Node} in this {@literal Graph} with the given name.
	 *
	 * @param name
	 * 		the name of the new {@literal Node}
	 *
	 * @return the new {@literal Node}
	 */
	public Node<T> node(String name) {
		return node(name, null);
	}

	/**
	 * Create a new {@literal Node} in this {@literal Graph} with the given name and using the given {@literal
	 * Dispatcher}
	 * when dispatching tasks.
	 *
	 * @param name
	 * 		the name of the new {@literal Node}
	 * @param dispatcher
	 * 		the {@literal Dispatcher} to use
	 *
	 * @return the new {@literal Node}
	 */
	public Node<T> node(String name, Dispatcher dispatcher) {
		Assert.isTrue(!nodes.containsKey(name), "A Node is already created with name '" + name + "'");
		Dispatcher d = (null != dispatcher ? dispatcher : defaultDispatcher);
		Reactor reactor = Reactors.reactor(env, d);
		Node<T> node = new Node<T>(name, this, reactor, null);
		nodes.put(name, node);
		return node;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void accept(T t) {
		if(null == startNode && nodes.size() == 1) {
			startNode = nodes.values().iterator().next();
		}
		Assert.notNull(startNode, "No initial starting Node specified. Call Graph.startNode(String) to set one.");
		startNode.notifyValue(eventFactory.get().setData(t));
	}

	BatchFactorySupplier<Event> getEventFactory() {
		return eventFactory;
	}

	Node<T> getNode(String name) {
		Assert.isTrue(nodes.containsKey(name), "No Node named '" + name + "' found.");
		return nodes.get(name);
	}

	@Override
	public String toString() {
		return "Graph{" +
				"nodes=" + nodes +
				", startNode=" + startNode +
				'}';
	}

}
