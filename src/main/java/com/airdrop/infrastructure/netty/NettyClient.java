package com.airdrop.infrastructure.netty;

import com.airdrop.domain.model.FileTask;
import com.airdrop.domain.model.Peer;
import com.airdrop.usecase.gateway.NetworkGateway.FileTransferListener;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.CharsetUtil;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.nio.charset.StandardCharsets;
import java.util.Enumeration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class NettyClient {
    private static final String MULTICAST_IP = "224.0.0.167";
    private static final int MULTICAST_PORT = 53333;

    private final EventLoopGroup workerGroup = new NioEventLoopGroup();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private Channel udpChannel;
    
    // Track active TCP channels for cancellation
    private final ConcurrentHashMap<String, Channel> activeTransfers = new ConcurrentHashMap<>();

    public void startUdpMulticastPublisher(String localPeerName, int tcpPort) throws InterruptedException, IOException {
        Bootstrap b = new Bootstrap();
        b.group(workerGroup)
         .channel(NioDatagramChannel.class)
         .option(ChannelOption.SO_REUSEADDR, true)
         .handler(new ChannelInboundHandlerAdapter()); // Dummy handler

        NetworkInterface ni = findMulticastInterface();
        if (ni == null) {
            throw new IOException("No suitable network interface found for multicast publisher.");
        }

        NioDatagramChannel ch = (NioDatagramChannel) b.bind(0).sync().channel();
        udpChannel = ch;

        // Schedule periodic broadcast every 2 seconds
        scheduler.scheduleAtFixedRate(() -> {
            try {
                String message = "AIRDROP_PING:" + localPeerName + ":" + tcpPort;
                ByteBuf buf = Unpooled.copiedBuffer(message, CharsetUtil.UTF_8);
                DatagramPacket packet = new DatagramPacket(buf, new InetSocketAddress(MULTICAST_IP, MULTICAST_PORT));
                udpChannel.writeAndFlush(packet);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, 0, 2, TimeUnit.SECONDS);
    }

    public void stopUdpPublisher() {
        scheduler.shutdownNow();
        if (udpChannel != null) {
            udpChannel.close();
        }
    }

    public void stop() {
        stopUdpPublisher();
        workerGroup.shutdownGracefully();
    }

    public void sendFile(String localPeerName, Peer target, FileTask task, FileTransferListener listener) {
        Bootstrap b = new Bootstrap();
        b.group(workerGroup)
         .channel(NioSocketChannel.class)
         .option(ChannelOption.SO_KEEPALIVE, true)
         .handler(new ChannelInitializer<SocketChannel>() {
             @Override
             protected void initChannel(SocketChannel ch) {
                 ch.pipeline().addLast(new FileSenderHandler(localPeerName, task, listener, activeTransfers));
             }
         });

        b.connect(target.getIp(), target.getPort()).addListener((ChannelFutureListener) future -> {
            if (!future.isSuccess()) {
                listener.onSendFailed(task.getId(), future.cause());
            }
        });
    }

    public void cancelTransfer(String taskId) {
        Channel channel = activeTransfers.get(taskId);
        if (channel != null) {
            channel.close();
        }
    }

    private NetworkInterface findMulticastInterface() throws IOException {
        Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
        while (interfaces.hasMoreElements()) {
            NetworkInterface ni = interfaces.nextElement();
            if (ni.isUp() && ni.supportsMulticast() && !ni.isLoopback()) {
                return ni;
            }
        }
        return NetworkInterface.getByInetAddress(java.net.InetAddress.getLocalHost());
    }

    // --- Inner Handler ---

    private static class FileSenderHandler extends ChannelInboundHandlerAdapter {
        private final String localPeerName;
        private final FileTask task;
        private final FileTransferListener listener;
        private final ConcurrentHashMap<String, Channel> activeTransfers;

        public FileSenderHandler(String localPeerName, FileTask task, FileTransferListener listener, ConcurrentHashMap<String, Channel> activeTransfers) {
            this.localPeerName = localPeerName;
            this.task = task;
            this.listener = listener;
            this.activeTransfers = activeTransfers;
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            activeTransfers.put(task.getId(), ctx.channel());
            listener.onSendStarted(task.getId());

            File file = new File(task.getFilePath());
            if (!file.exists() || !file.isFile()) {
                listener.onSendFailed(task.getId(), new FileNotFoundException("File not found: " + task.getFilePath()));
                ctx.close();
                return;
            }

            // 1. Send metadata frame: [4-byte length][Metadata String]
            String metadata = task.getId() + "|" + task.getFileName() + "|" + file.length() + "|" + localPeerName;
            byte[] metadataBytes = metadata.getBytes(StandardCharsets.UTF_8);
            
            ByteBuf headerBuf = ctx.alloc().buffer(4 + metadataBytes.length);
            headerBuf.writeInt(metadataBytes.length);
            headerBuf.writeBytes(metadataBytes);
            ctx.writeAndFlush(headerBuf);

            // 2. Send file data using Zero-Copy (DefaultFileRegion)
            io.netty.channel.DefaultFileRegion region = new io.netty.channel.DefaultFileRegion(file, 0, file.length());
            ChannelFuture sendFileFuture = ctx.writeAndFlush(region);

            sendFileFuture.addListener(new ChannelProgressiveFutureListener() {
                @Override
                public void operationProgressed(ChannelProgressiveFuture future, long progress, long total) {
                    listener.onSendProgress(task.getId(), progress, total < 0 ? file.length() : total);
                }

                @Override
                public void operationComplete(ChannelProgressiveFuture future) {
                    activeTransfers.remove(task.getId());
                    if (future.isSuccess()) {
                        listener.onSendCompleted(task.getId());
                        ctx.close(); // Close connection after sending completes
                    } else {
                        listener.onSendFailed(task.getId(), future.cause());
                        ctx.close();
                    }
                }
            });
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            activeTransfers.remove(task.getId());
            listener.onSendFailed(task.getId(), cause);
            ctx.close();
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            activeTransfers.remove(task.getId());
            super.channelInactive(ctx);
        }
    }
}
