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

package reactor.support;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A thread factory that creates named daemon threads. Each thread created by this class
 * will have a different name due to a count of the number of threads  created thus far
 * being included in the name of each thread. This count is held statically to ensure
 * different thread names across different instances of this class.
 *
 * @see Thread#setDaemon(boolean)
 * @see Thread#setName(String)
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class NamedDaemonThreadFactory implements ThreadFactory {

	private static final AtomicInteger COUNTER = new AtomicInteger(0);

	private final String prefix;
	private final ClassLoader contextClassLoader;

	/**
	 * Creates a new thread factory that will name its threads &lt;prefix&gt;-&lt;n&gt;, where
	 * &lt;prefix&gt; is the given {@code prefix} and &lt;n&gt; is the count of threads
	 * created thus far by this class.
	 *
	 * @param prefix The thread name prefix
	 */
	public NamedDaemonThreadFactory(String prefix) {
		this(prefix, null);
	}

	/**
	 * Creates a new thread factory that will name its threads &lt;prefix&gt;-&lt;n&gt;, where
	 * &lt;prefix&gt; is the given {@code prefix} and &lt;n&gt; is the count of threads
	 * created thus far by this class. If the contextClassLoader parameter is not null it will assign it to the forged
	 * Thread
	 *
	 * @param prefix The thread name prefix
	 * @param contextClassLoader An optional classLoader to assign to thread
	 */
	public NamedDaemonThreadFactory(String prefix, ClassLoader contextClassLoader) {
		this.prefix = prefix;
		this.contextClassLoader = contextClassLoader;
	}

	@Override
	public Thread newThread(Runnable runnable) {
		Thread t = new Thread(runnable);
		t.setName(prefix + "-" + COUNTER.incrementAndGet());
		t.setDaemon(true);
		if(contextClassLoader != null){
			t.setContextClassLoader(contextClassLoader);
		}
		return t;
	}

}
