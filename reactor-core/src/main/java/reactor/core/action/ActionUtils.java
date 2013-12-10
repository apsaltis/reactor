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
package reactor.core.action;

import reactor.core.Reactor;
import reactor.event.Event;
import reactor.event.registry.Registration;
import reactor.function.Consumer;

/**
 * @author Stephane Maldini
 */
public abstract class ActionUtils {


	public static String browseReactor(Reactor reactor, Object successKey, Object errorKey) {
		return browseReactor(reactor, successKey, errorKey, null);
	}

	public static String browseReactor(Reactor reactor, Object successKey) {
		return browseReactor(reactor, successKey, null, null);
	}

	public static String browseReactor(Reactor reactor) {
		ActionVisitor actionVisitor = new ActionVisitor(reactor, true);
		actionVisitor.loopActions(reactor.getConsumerRegistry(), 1, "accept");
		return actionVisitor.toString();
	}

	public static String browseReactor(Reactor reactor, Object successKey, Object failureKey,
	                                   Object flushKey) {
		ActionVisitor actionVisitor = new ActionVisitor(reactor, true);
		actionVisitor.drawReactorConsumers(reactor, successKey, failureKey, flushKey, 1);
		return actionVisitor.toString();
	}

	public static class ActionVisitor {

		final private boolean       visitFailures;
		final private StringBuilder appender;

		private ActionVisitor(Reactor reactor, boolean visitFailures) {
			this.appender = new StringBuilder("\nreactor(" + reactor.getId() + ")");
			this.visitFailures = visitFailures;
		}

		private ActionVisitor(Reactor reactor) {
			this(reactor, false);
		}

		private ActionVisitor drawReactorConsumers(Reactor reactor, Object successKey, Object failureKey, Object flushKey,
		                                              int d) {

			if (successKey != null) {
				loopActions(reactor.getConsumerRegistry().select(successKey), d, "accept");
			}

			if (flushKey != null) {
				loopActions(reactor.getConsumerRegistry().select(flushKey), d, "flush");
			}

			if (visitFailures && failureKey != null)
				loopActions(reactor.getConsumerRegistry().select(failureKey), d, "fail");

			return this;
		}

		private void loopActions(Iterable<Registration<? extends Consumer<? extends Event<?>>>> operations, int d,
		                         String marker) {
			for (Registration<?> registration : operations) {

				appender.append("\n");
				for (int i = 0; i < d; i++)
					appender.append("|   ");
				appender.append("|____" + marker + ":");

				appender.append(registration.getObject().getClass().getSimpleName().isEmpty() ? registration.getObject() :
						registration.getObject()
								.getClass()
								.getSimpleName());

				if (Action.class.isAssignableFrom(registration.getObject().getClass())) {
					Action<?> operation = ((Action) registration.getObject());

					renderBatch(operation, d);
					renderFilter(operation, d);

					drawReactorConsumers(
							(Reactor) operation.getObservable(),
							operation.getSuccessKey(),
							operation.getFailureKey(),
							null,
							d + 1
					);
				}
			}
		}

		private void renderFilter(Object consumer, int d) {
			if (FilterAction.class.isAssignableFrom(consumer.getClass())) {
				FilterAction operation = (FilterAction) consumer;

				if (operation.getElseObservable() != null) {
					loopActions(((Reactor)operation.getElseObservable()).getConsumerRegistry()
					                                                    .select(operation.getElseSuccess()),
					            d + 1, "else");
				}
			}
		}


		private void renderBatch(Object consumer, int d) {
			if (BatchAction.class.isAssignableFrom(consumer.getClass())) {
				BatchAction operation = (BatchAction) consumer;
				appender.append(" accepted:" + operation.getAcceptCount());
				appender.append("|errors:" + operation.getErrorCount());
				appender.append("|batchSize:" + operation.getBatchSize());

				loopActions(((Reactor)operation.getObservable()).getConsumerRegistry().select(operation.getFirstKey()),
				            d + 1, "first");
				loopActions(((Reactor)operation.getObservable()).getConsumerRegistry().select(operation.getFlushKey()),
				            d + 1, "flush");
			}
		}

		@Override
		public String toString() {
			return appender.toString();
		}
	}
}
