package com.arturmkrtchyan;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.pool.AbstractChannelPoolHandler;
import io.netty.channel.pool.ChannelPool;
import io.netty.channel.pool.FixedChannelPool;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.codec.Delimiters;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import io.netty.util.CharsetUtil;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

public class HelloClient {
    public static final AttributeKey<CompletableFuture<String>> FUTURE = AttributeKey.valueOf("future");

    private final StringDecoder stringDecoder = new StringDecoder(CharsetUtil.UTF_8);
    private final StringEncoder stringEncoder = new StringEncoder(CharsetUtil.UTF_8);

    private ChannelPool channelPool;
    private EventLoopGroup eventLoopGroup;

    private void connect(String host, int port, int numberOfThreads, int numberOfConnections) {
        eventLoopGroup = new NioEventLoopGroup(numberOfThreads);

        Bootstrap bootstrap = new Bootstrap();
        bootstrap.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        bootstrap.option(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, 32 * 1024);
        bootstrap.option(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, 8 * 1024);
        bootstrap.option(ChannelOption.TCP_NODELAY, true);

        bootstrap.group(eventLoopGroup).channel(NioSocketChannel.class).remoteAddress(host, port);

        channelPool = new FixedChannelPool(bootstrap, new AbstractChannelPoolHandler() {
            @Override
            public void channelCreated(Channel ch) {
                ChannelPipeline pipeline = ch.pipeline();
                // decoders
                pipeline.addLast("framer", new DelimiterBasedFrameDecoder(8192, Delimiters.lineDelimiter()));
                pipeline.addLast("stringDecoder", stringDecoder);

                // encoders
                pipeline.addLast("stringEncoder", stringEncoder);

                // business logic handler
                pipeline.addLast("clientHandler", new HelloClientHandler(channelPool));
            }
        }, numberOfConnections);
    }

    private void disconnect() throws Exception {
        eventLoopGroup.shutdownGracefully().sync();
    }

    public CompletableFuture<String> send(final String message) {
        CompletableFuture<String> future = new CompletableFuture<>();

        Future<Channel> channelFuture = channelPool.acquire();
        channelFuture.addListener(new FutureListener<Channel>() {
            @Override
            public void operationComplete(Future<Channel> f) {
                if (f.isSuccess()) {
                    Channel channel = f.getNow();
                    channel.attr(HelloClient.FUTURE).set(future);
                    channel.writeAndFlush(message, channel.voidPromise());
                }
            }
        });
        return future;
    }

    public static void main(String[] args) throws Exception {

        HelloClient client = new HelloClient();
        final String host = System.getProperty("host", "localhost");
        final int port = Integer.valueOf(System.getProperty("port", "7657"));
        final int threads = Integer.valueOf(System.getProperty("threads", "1"));
        final int connections = Integer.valueOf(System.getProperty("connections", "10"));
        final int messages = Integer.valueOf(System.getProperty("messages", "100000"));
        final int iterations = Integer.valueOf(System.getProperty("iterations", "10"));
        client.connect(host, port, threads, connections);

        IntStream.range(0, iterations).forEach(iteration -> {
            System.out.println("---------------------Iteration ->" + iteration + "---------------------");
            System.out.println("Asynchronously sending " + (messages/1000) + "K messages.");
            CompletableFuture[] futures = new CompletableFuture[messages];

            long start = System.currentTimeMillis();

            IntStream.range(0, messages).forEach(index -> {
                try {
                    CompletableFuture<String> future = client.send("Hello from client\n");
                    futures[index] = future;
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });

            System.out.println("It took " + (System.currentTimeMillis() - start) +
                    "ms to asynchronously send " + (messages/1000) + "K messages.");

            System.out.println("Waiting to get responses from server.");
            CompletableFuture f = CompletableFuture.allOf(futures);
            try {
                f.get(60, TimeUnit.SECONDS);
            } catch (Exception e) {
                e.printStackTrace();
            }

            System.out.println("Server handles " + (messages/(System.currentTimeMillis() - start)) +
                    "K messages per second");
        });

        client.disconnect();

    }
}


class HelloClientHandler extends SimpleChannelInboundHandler<String> {

    private final ChannelPool channelPool;

    HelloClientHandler(ChannelPool channelPool) {
        this.channelPool = channelPool;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, String msg) throws Exception {
        Attribute<CompletableFuture<String>> futureAttribute = ctx.channel().attr(HelloClient.FUTURE);
        CompletableFuture<String> future = futureAttribute.getAndRemove();

        channelPool.release(ctx.channel(), ctx.channel().voidPromise());
        future.complete(msg);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        Attribute<CompletableFuture<String>> futureAttribute = ctx.channel().attr(HelloClient.FUTURE);
        CompletableFuture<String> future = futureAttribute.getAndRemove();
        cause.printStackTrace();
        channelPool.release(ctx.channel());
        ctx.close();
        future.completeExceptionally(cause);
    }
}
