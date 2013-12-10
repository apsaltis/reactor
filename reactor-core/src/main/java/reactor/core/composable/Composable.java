/*
 * Copyright (c) 2011-2013 GoPivotal, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.composable;

import reactor.core.Observable;
import reactor.core.Reactor;
import reactor.event.Event;
import reactor.event.dispatch.Dispatcher;
import reactor.event.selector.Selector;
import reactor.event.selector.Selectors;
import reactor.function.Consumer;
import reactor.function.Function;
import reactor.function.Predicate;
import reactor.operations.*;
import reactor.tuple.Tuple2;
import reactor.util.Assert;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Abstract base class for components designed to provide a succinct API for working with future values. Provides base
 * functionality and an internal contract for subclasses that make use of the {@link #map(reactor.function.Function)}
 * and {@link #filter(reactor.function.Predicate)} methods.
 *
 * @param <T> The type of the values
 * @author Stephane Maldini
 * @author Jon Brisbin
 * @author Andy Wilkinson
 */
public abstract class Composable<T> implements OperationPipe<T> {

	private final Tuple2<Selector, Object> accept;
	private final Tuple2<Selector, Object> error = Selectors.$();
	private final Tuple2<Selector, Object> flush = Selectors.$();

	private final Observable    events;
	private final Composable<?> parent;

	protected <U> Composable(@Nullable Dispatcher dispatcher, @Nullable Composable<U> parent) {
		Assert.state(dispatcher != null || parent != null, "One of 'dispatcher' or 'parent'  cannot be null.");
		this.events = parent == null ?
				new Reactor(dispatcher) :
				new Reactor(dispatcher,
						((Reactor) parent.getObservable()).getEventRouter(),
						((Reactor) parent.getObservable()).getConsumerRegistry());

		this.parent = parent;
		this.accept = Selectors.$();
		if (parent != null) {
			events.on(parent.error.getT1(),
					new ConnectOperation<Throwable>(events, error.getT2(), null));
		}
	}

	protected Composable(@Nonnull Observable observable, @Nullable Tuple2<Selector, Object> acceptSelector) {
		Assert.notNull(observable, "'observable' cannot be null.");
		this.events = observable;
		this.accept = null == acceptSelector ? Selectors.$() : acceptSelector;
		this.parent = null;
	}


	/**
	 * Assign an error handler to exceptions of the given type.
	 *
	 * @param exceptionType the type of exceptions to handle
	 * @param onError       the error handler for each exception
	 * @param <E>           type of the exception to handle
	 * @return {@literal this}
	 */
	public <E extends Throwable> Composable<T> when(@Nonnull final Class<E> exceptionType,
	                                                @Nonnull final Consumer<E> onError) {
		this.events.on(error.getT1(), new BaseOperation<E>(getObservable(), getAccept()) {
			@Override
			protected void doOperation(Event<E> e) {
				if (Selectors.T(exceptionType).matches(e.getData().getClass())) {
					onError.accept(e.getData());
				}
			}
		});
		return this;
	}

	/**
	 * Attach another {@code Composable} to this one that will cascade the value or error received by this {@code
	 * Composable} into the next.
	 *
	 * @param composable the next {@code Composable} to cascade events to
	 * @return {@literal this}
	 */
	public Composable<T> connect(@Nonnull final Composable<T> composable) {
		this.consume(composable);
		events.on(error.getT1(), new ConnectOperation<Throwable>(composable.events, composable.error.getT2(), null));
		return this;
	}

	/**
	 * Attach another {@code Composable} to this one that will only cascade the value received by this {@code
	 * Composable} into the next.
	 *
	 * @param composable the next {@code Composable} to cascade events to
	 * @return {@literal this}
	 */
	public Composable<T> consume(@Nonnull final Composable<T> composable) {
		if (composable == this) {
			throw new IllegalArgumentException("Trying to consume itself, leading to erroneous recursive calls");
		}
		addOperation(new ConnectOperation<T>(composable.events, composable.accept.getT2(), composable.error.getT2()));

		return this;
	}

	/**
	 * Attach a {@link Consumer} to this {@code Composable} that will consume any values accepted by this {@code
	 * Composable}.
	 *
	 * @param consumer the conumer to invoke on each value
	 * @return {@literal this}
	 */
	public Composable<T> consume(@Nonnull final Consumer<T> consumer) {
		addOperation(new CallbackOperation<T>(consumer, events, error.getT2()));
		return this;
	}

	/**
	 * Attach a {@link Consumer<Event>} to this {@code Composable} that will consume any values accepted by this {@code
	 * Composable}.
	 *
	 * @param consumer the conumer to invoke on each value
	 * @return {@literal this}
	 */
	public Composable<T> consumeEvent(@Nonnull final Consumer<Event<T>> consumer) {
		addOperation(new CallbackEventOperation<T>(consumer, events, error.getT2()));
		return this;
	}

	/**
	 * Pass values accepted by this {@code Composable} into the given {@link Observable}, notifying with the given key.
	 *
	 * @param key        the key to notify on
	 * @param observable the {@link Observable} to notify
	 * @return {@literal this}
	 */
	public Composable<T> consume(@Nonnull final Object key, @Nonnull final Observable observable) {
		addOperation(new ConnectOperation<T>(observable, key, null));
		return this;
	}

	/**
	 * Assign the given {@link Function} to transform the incoming value {@code T} into a {@code V} and pass it into
	 * another {@code Composable}.
	 *
	 * @param fn  the transformation function
	 * @param <V> the type of the return value of the transformation function
	 * @return a new {@code Composable} containing the transformed values
	 */
	public <V> Composable<V> map(@Nonnull final Function<T, V> fn) {
		Assert.notNull(fn, "Map function cannot be null.");
		final Deferred<V, ? extends Composable<V>> d = createDeferred();
		addOperation(new MapOperation<T, V>(
				fn,
				d.compose().getObservable(),
				d.compose().getAccept().getT2(),
				error.getT2()));
		return d.compose();
	}

	/**
	 * Assign the given {@link Function} to transform the incoming value {@code T} into a {@code Composable<V>} and pass
	 * it into another {@code Composable}.
	 *
	 * @param fn  the transformation function
	 * @param <V> the type of the return value of the transformation function
	 * @return a new {@code Composable} containing the transformed values
	 */
	public <V> Composable<V> flatMap(@Nonnull final Function<T, Composable<V>> fn) {
		Assert.notNull(fn, "FlatMap function cannot be null.");
		final Deferred<V, ? extends Composable<V>> d = createDeferred();
		addOperation(new FlatMapOperation<T, V, Composable<V>>(
				fn,
				d.compose().getObservable(),
				d.compose().getAccept().getT2(),
				error.getT2()));
		return d.compose();
	}

	/**
	 * Evaluate each accepted value against the given predicate {@link Function}. If the predicate test succeeds, the
	 * value is passed into the new {@code Composable}. If the predicate test fails, an exception is propagated into the
	 * new {@code Composable}.
	 *
	 * @param fn the predicate {@link Function} to test values against
	 * @return a new {@code Composable} containing only values that pass the predicate test
	 */
	public Composable<T> filter(@Nonnull final Function<T, Boolean> fn) {
		return filter(new Predicate<T>() {
			@Override
			public boolean test(T t) {
				return fn.apply(t);
			}
		}, null);
	}

	/**
	 * Evaluate each accepted value against the given {@link Predicate}. If the predicate test succeeds, the value is
	 * passed into the new {@code Composable}. If the predicate test fails, the value is ignored.
	 *
	 * @param p the {@link Predicate} to test values against
	 * @return a new {@code Composable} containing only values that pass the predicate test
	 */
	public Composable<T> filter(@Nonnull final Predicate<T> p) {
		return filter(p, null);
	}

	/**
	 * Evaluate each accepted value against the given {@link Predicate}. If the predicate test succeeds, the value is
	 * passed into the new {@code Composable}. If the predicate test fails, the value is propagated into the {@code
	 * elseComposable}.
	 *
	 * @param p              the {@link Predicate} to test values against
	 * @param elseComposable the optional {@link reactor.core.composable.Composable} to pass rejected values
	 * @return a new {@code Composable} containing only values that pass the predicate test
	 */
	public Composable<T> filter(@Nonnull final Predicate<T> p, final Composable<T> elseComposable) {
		final Deferred<T, ? extends Composable<T>> d = createDeferred();
		addOperation(new FilterOperation<T>(p, d.compose().getObservable(), d.compose().getAccept().getT2(), error.getT2(),
				elseComposable != null ? elseComposable.events : null,
				elseComposable != null ? elseComposable.accept.getT2() : null));
		return d.compose();
	}

	/**
	 * Flush any cached or unprocessed values through this {@literal Stream}.
	 *
	 * @return {@literal this}
	 */
	@Override
	public Composable<T> flush() {
		Composable<?> that = this;
		while (that.parent != null) {
			that = that.parent;
		}

		that.notifyFlush();
		return this;
	}

	public String debug() {
		Composable<?> that = this;
		while (that.parent != null) {
			that = that.parent;
		}
		return OperationUtils.browseReactor((Reactor) that.events
		);
	}

	/**
	 * Consume events with the passed {@code Operation}
	 *
	 * @param operation the operation listening for values
	 */
	public Composable<T> addOperation(Operation<T> operation) {
		this.events.on(accept.getT1(), operation);
		return this;
	}

	/**
	 * Notify this {@code Composable} that a flush is being requested by this {@code Composable}.
	 */
	void notifyFlush() {
		events.notify(flush.getT2(), new Event<Void>(null));
	}

	/**
	 * Notify this {@code Composable} that a value is being accepted by this {@code Composable}.
	 *
	 * @param value the value to accept
	 */
	void notifyValue(T value) {
		notifyValue(Event.wrap(value));
	}

	void notifyValue(Event<T> value) {
		events.notify(accept.getT2(), value);
	}

	/**
	 * Notify this {@code Composable} that an error is being propagated through this {@code Composable}.
	 *
	 * @param error the error to propagate
	 */
	void notifyError(Throwable error) {
		events.notify(this.error.getT2(), Event.wrap(error));
	}

	/**
	 * Create a {@link Deferred} that is compatible with the subclass of {@code Composable} in use.
	 *
	 * @param <V> type the {@code Composable} handles
	 * @param <C> type of the {@code Composable} returned by the {@link Deferred#compose()} method.
	 * @return a new {@link Deferred} backed by a {@code Composable} compatible with the current subclass.
	 */
	protected abstract <V, C extends Composable<V>> Deferred<V, C> createDeferred();


	/**
	 * Get the current {@link Observable}.
	 *
	 * @return
	 */
	protected Observable getObservable() {
		return events;
	}

	/**
	 * Get the anonymous {@link Selector} and notification key {@link Tuple2} for doing accepts.
	 *
	 * @return
	 */
	protected Tuple2<Selector, Object> getAccept() {
		return this.accept;
	}

	/**
	 * Get the anonymous {@link Selector} and notification key {@link Tuple2} for doing errors.
	 *
	 * @return
	 */
	protected Tuple2<Selector, Object> getError() {
		return this.error;
	}

	/**
	 * Get the anonymous {@link Selector} and notification key {@link Tuple2} for doing flushes.
	 *
	 * @return
	 */
	protected Tuple2<Selector, Object> getFlush() {
		return this.flush;
	}

	/**
	 * Get the parent {@link Composable} for callback callback.
	 *
	 * @return
	 */
	protected Composable<?> getParent() {
		return this.parent;
	}

}
