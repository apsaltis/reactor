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
package reactor.core.composable.spec;

import reactor.core.Environment;
import reactor.core.Observable;
import reactor.core.Reactor;
import reactor.core.spec.support.DispatcherComponentSpec;
import reactor.event.dispatch.Dispatcher;
import reactor.event.dispatch.SynchronousDispatcher;
import reactor.event.selector.Selector;
import reactor.tuple.Tuple2;

/**
 * A helper class for specifying a bounded {@link reactor.core.composable.Stream}. {@link #each} must be called to
 * provide the stream with its values.
 *
 * @param <SPEC>   The ComposableSpec subclass
 * @param <TARGET> The type that this spec will create
 * @author Stephane Maldini
 */
public abstract class ComposableSpec<SPEC extends ComposableSpec<SPEC, TARGET>, TARGET> extends DispatcherComponentSpec<SPEC,
		TARGET> {

	private boolean newReactor = false;
	private Observable               observable;
	private Tuple2<Selector, Object> acceptSelector;

	/**
	 * Configures the Composable to use an anonymous reactor instead of the environment root one
	 *
	 * @return {@code this}
	 */
	@SuppressWarnings("unchecked")
	public SPEC fork(boolean newReactor) {
		this.newReactor = newReactor;
		return (SPEC) this;
	}

	/**
	 * Configures the Composable to reuse an explicit selector/key rather than the internal anonymous generated one.
	 *
	 * @param acceptSelector The selector tuple to listen/publish to
	 * @return {@code this}
	 */
	@SuppressWarnings("unchecked")
	SPEC acceptSelector(final Tuple2<Selector, Object> acceptSelector) {
		this.acceptSelector = acceptSelector;
		return (SPEC) this;
	}

	/**
	 * Configures the Composable to reuse an explicit observable rather than the internal anonymous generated one.
	 *
	 * @param observable The observable to listen/publish to
	 * @return {@code this}
	 */
	@SuppressWarnings("unchecked")
	SPEC observable(final Observable observable) {
		this.observable = observable;
		return (SPEC) this;
	}


	@Override
	protected TARGET configure(final Dispatcher dispatcher, Environment env) {
		if (observable == null) {
			if(newReactor || env == null){
				observable = new Reactor(dispatcher == null ? new SynchronousDispatcher() : dispatcher).control();
			}else{
				observable = env.getRootReactor();
			}
		}
		return createComposable(env, observable, acceptSelector);
	}

	protected abstract TARGET createComposable(Environment env, Observable observable, Tuple2<Selector, Object> accept);

}
