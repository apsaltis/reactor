package reactor.tcp.zmq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;
import reactor.core.Environment;
import reactor.core.Reactor;
import reactor.core.composable.Deferred;
import reactor.core.composable.Promise;
import reactor.core.composable.spec.Promises;
import reactor.event.selector.ObjectSelector;
import reactor.event.selector.Selector;
import reactor.function.Consumer;
import reactor.function.Supplier;
import reactor.io.Buffer;
import reactor.io.encoding.Codec;
import reactor.queue.BlockingQueueFactory;
import reactor.support.NamedDaemonThreadFactory;
import reactor.tcp.TcpConnection;
import reactor.tcp.TcpServer;
import reactor.tcp.config.ServerSocketOptions;
import reactor.tcp.config.SslOptions;
import reactor.tuple.Tuple2;
import reactor.util.Assert;
import reactor.util.UUIDUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static reactor.event.selector.Selectors.$;

/**
 * @author Jon Brisbin
 */
public class ZeroMQTcpServer<IN, OUT> extends TcpServer<IN, OUT> {

	private static final Logger LOG = LoggerFactory.getLogger(ZeroMQTcpServer.class);

	private final UUID                     id       = UUIDUtils.create();
	private final Object                   monitor  = new Object();
	private final Tuple2<Selector, Object> shutdown = $();
	private final AtomicBoolean            started  = new AtomicBoolean(false);
	private final    InetSocketAddress   listenAddress;
	private final    ServerSocketOptions options;
	private final    SslOptions          sslOpts;
	private final    int                 selectThreadCount;
	private final    int                 ioThreadCount;
	private final    ExecutorService     workers;
	private final    BlockingQueue<ZMsg> incoming;
	private volatile ZMQ.Socket          frontend;

	public ZeroMQTcpServer(@Nonnull Environment env,
	                       @Nonnull Reactor reactor,
	                       @Nullable InetSocketAddress listenAddress,
	                       ServerSocketOptions opts,
	                       SslOptions sslOpts,
	                       @Nullable Codec<Buffer, IN, OUT> codec,
	                       @Nonnull Collection<Consumer<TcpConnection<IN, OUT>>> connectionConsumers) {
		super(env, reactor, listenAddress, opts, sslOpts, codec, connectionConsumers);
		this.listenAddress = (null == listenAddress
		                      ? InetSocketAddress.createUnresolved("0.0.0.0", 3000)
		                      : listenAddress);
		Assert.notNull(opts, "ServerSocketOptions cannot be null");
		this.options = opts;
		this.sslOpts = sslOpts;

		this.ioThreadCount = env.getProperty("reactor.tcp.ioThreadCount", Integer.class, 1);
		this.selectThreadCount = env.getProperty("reactor.tcp.selectThreadCount", Integer.class, 1);
		this.workers = Executors.newCachedThreadPool(new NamedDaemonThreadFactory("zmq-tcp"));
		this.incoming = BlockingQueueFactory.createQueue();
	}

	@Override
	public synchronized TcpServer<IN, OUT> start(@Nullable final Consumer<Void> started) {
		if(this.started.get()) {
			if(LOG.isWarnEnabled()) {
				LOG.warn("ZeroMQ server already started...");
			}
			return this;
		}

		workers.submit(new Runnable() {
			@Override
			public void run() {
				ZContext zctx = new ZContext(ioThreadCount);
				frontend = zctx.createSocket(ZMQ.ROUTER);
				frontend.setIdentity(id.toString().getBytes());
				frontend.setReceiveBufferSize(options.rcvbuf());
				frontend.setSendBufferSize(options.sndbuf());

				if(LOG.isInfoEnabled()) {
					LOG.info("Starting ZeroMQ server on {}...", listenAddress);
				}
				String addr = String.format("tcp://%s:%s", listenAddress.getHostString(), listenAddress.getPort());
				frontend.bind(addr);

				ZMQ.Socket backend = zctx.createSocket(ZMQ.DEALER);
				backend.bind("inproc://" + id.toString());
				for(int i = 0; i < selectThreadCount; i++) {
					workers.submit(new ZeroMQDealerWorker(zctx, id));
				}

				if(null != started) {
					started.accept(null);
				}
				ZeroMQTcpServer.this.started.set(true);

				ZMQ.proxy(frontend, backend, null);

				zctx.destroy();
			}
		});

		return this;
	}

	@Override
	public Promise<Void> shutdown() {
		final Deferred<Void, Promise<Void>> d = Promises.defer(env, getReactor().getDispatcher());
		getReactor().notify(shutdown.getT2());
		d.accept((Void)null);
		workers.shutdown();
		started.set(false);
		return d.compose();
	}

	@Override
	protected <C> TcpConnection<IN, OUT> select(@Nonnull C channel) {
		synchronized(monitor) {
			return super.select(channel);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	protected <C> TcpConnection<IN, OUT> createConnection(C channel) {
		return new ZeroMQTcpConnection<>(env,
		                                 getCodec(),
		                                 getReactor().getDispatcher(),
		                                 getReactor(),
		                                 (byte[])channel);
	}

	private class ZeroMQDealerWorker implements Runnable {
		private final ZContext zctx;
		private final UUID     id;

		private ZeroMQDealerWorker(ZContext zctx, UUID id) {
			this.zctx = zctx;
			this.id = id;
		}

		@Override
		public void run() {
			final ZMQ.Socket worker = zctx.createSocket(ZMQ.DEALER);
			worker.connect("inproc://" + id.toString());

			while(!Thread.currentThread().isInterrupted()) {
				ZMsg msg = ZMsg.recvMsg(worker);
				ZFrame address = msg.pop();
				ZFrame content = msg.pop();
				if(LOG.isTraceEnabled()) {
					LOG.trace("{} received from {}", content, address);
				}
				if(null == content) {
					msg.destroy();
					return;
				}

				ZeroMQTcpConnection<IN, OUT> conn = (ZeroMQTcpConnection<IN, OUT>)select(address.getData());
				conn.setSocket(worker);
				conn.read(Buffer.wrap(content.getData()));

				msg.destroy();
			}
		}
	}

	private static abstract class BaseSocketSupplier implements Supplier<ZMQ.Socket> {
		protected final ZContext            zctx;
		protected final ServerSocketOptions options;

		private BaseSocketSupplier(ZContext zctx, ServerSocketOptions options) {
			this.zctx = zctx;
			this.options = options;
		}

		@Override
		public ZMQ.Socket get() {
			ZMQ.Socket socket = createSocket();
			socket.setReceiveBufferSize(options.rcvbuf());
			socket.setSendBufferSize(options.sndbuf());
			if(options.keepAlive()) {
				socket.setTCPKeepAlive(1);
			} else {
				socket.setTCPKeepAlive(0);
			}
			socket.setBacklog(options.backlog());
			return socket;
		}

		protected abstract ZMQ.Socket createSocket();
	}

	private static class ReplySocketSupplier extends BaseSocketSupplier {
		private ReplySocketSupplier(ZContext zctx, ServerSocketOptions options) {
			super(zctx, options);
		}

		@Override
		protected ZMQ.Socket createSocket() {
			return zctx.createSocket(ZMQ.REP);
		}
	}

	private static class RouterSocketSupplier extends BaseSocketSupplier {
		private RouterSocketSupplier(ZContext zctx, ServerSocketOptions options) {
			super(zctx, options);
		}

		@Override
		protected ZMQ.Socket createSocket() {
			return zctx.createSocket(ZMQ.ROUTER);
		}
	}

	private static class ZFrameSelector extends ObjectSelector<ZFrame> {
		private ZFrameSelector(ZFrame zf) {
			super(zf);
		}

		@Override
		public boolean matches(Object key) {
			if(!(key instanceof ZFrame)) {
				return super.matches(key);
			} else {
				byte[] b = ((ZFrame)key).getData();
				return Arrays.equals(getObject().getData(), b);
			}
		}
	}

}
