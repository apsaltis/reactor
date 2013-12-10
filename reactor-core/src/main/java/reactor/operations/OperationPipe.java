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
package reactor.operations;

/**
 * Component that can be injected with {@link Operation}s
 *
 * @author Stephane Maldini
 */
public interface OperationPipe<T>{

	/**
	 * Consume events with the passed {@code Operation}
	 *
	 * @param operation the operation listening for values
	 */
	OperationPipe<T> addOperation(Operation<T> operation);

	/**
	 * Flush any cached or unprocessed values through this {@literal OperationPipe}.
	 *
	 * @return {@literal this}
	 */
	OperationPipe<T> flush();
}
