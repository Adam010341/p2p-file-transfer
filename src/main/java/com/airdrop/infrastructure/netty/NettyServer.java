package com.airdrop.infrastructure.netty;

import com.airdrop.domain.model.Peer;
import com.airdrop.usecase.port.out.FileTransferListener;
import com.airdrop.usecase.port.out.PeerDiscoveryListener;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.util.CharsetUtil;

import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.Enumeration;

public class NettyServer {
    private static final String MULTICAST_IP = "224.0.0.167";
    private static final int MULTICAST_PORT = 53333;

    private final EventLoopGroup bossGroup = new NioEventLoopGroup(1);
    private final EventLoopGroup workerGroup = new NioEventLoopGroup();
    
    private Channel tcpChannel;
    private Channel udpChannel;
    
    private int boundTcpPort = -1;

    public void startTcpServer(int port, FileTransferListener listener) throws InterruptedException {
        ServerBootstrap b = new ServerBootstrap();
        b.group(bossGroup, workerGroup)
         .channel(NioServerSocketChannel.class)
         .childHandler(new ChannelInitializer<SocketChannel>() {
             @Override
             protected void initChannel(SocketChannel ch) {
                 ChannelPipeline p = ch.pipeline();
                 // Length of metadata string is 4 bytes, prepended before metadata payload
                 p.addLast(new LengthFieldBasedFrameDecoder(1048576, 0, 4, 0, 4));
                 p.addLast(new MetadataHandler(listener));
             }
         });

        ChannelFuture f = b.bind(port).sync();
        tcpChannel = f.channel();
        boundTcpPort = ((InetSocketAddress) tcpChannel.localAddress()).getPort();
    }

    public int getBoundTcpPort() {
        return boundTcpPort;
    }

    public void startUdpMulticastListener(PeerDiscoveryListener listener) throws InterruptedException, IOException {
        Bootstrap b = new Bootstrap();
        b.group(workerGroup)
         .channel(NioDatagramChannel.class)
         .option(ChannelOption.SO_REUSEADDR, true)
         .handler(new SimpleChannelInboundHandler<DatagramPacket>() {
             @Override
             protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket packet) {
                 String msg = packet.content().toString(CharsetUtil.UTF_8);
                 if (msg.startsWith("AIRDROP_PING:")) {
                     String[] parts = msg.split(":");
                     if (parts.length >= 3) {
                         String name = parts[1];
                         int port = Integer.parseInt(parts[2]);
                         String ip = packet.sender().getAddress().getHostAddress();
                         Peer peer = new Peer(name, ip, port);
                         listener.onPeerDiscovered(peer);
                     }
                 }
             }
         });

        NetworkInterface ni = findMulticastInterface();
        if (ni == null) {
            throw new IOException("No suitable network interface found for multicast.");
        }

        NioDatagramChannel ch = (NioDatagramChannel) b.bind(MULTICAST_PORT).sync().channel();
        ch.joinGroup(new InetSocketAddress(MULTICAST_IP, MULTICAST_PORT), ni).sync();
        udpChannel = ch;
    }

    public void stop() {
        if (tcpChannel != null) tcpChannel.close();
        if (udpChannel != null) udpChannel.close();
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
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

    // --- Inner Handlers ---

    private static class MetadataHandler extends ChannelInboundHandlerAdapter {
        private final FileTransferListener listener;
        private boolean metadataReceived = false;

        public MetadataHandler(FileTransferListener listener) {
            this.listener = listener;
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            if (!metadataReceived) {
                ByteBuf buf = (ByteBuf) msg;
                try {
                    String metadata = buf.toString(StandardCharsets.UTF_8);
                    // Format: taskId|fileName|fileSize|senderName
                    String[] parts = metadata.split("\\|");
                    if (parts.length >= 4) {
                        String taskId = parts[0];
                        String fileName = parts[1];
                        long fileSize = Long.parseLong(parts[2]);
                        String senderName = parts[3];

                        Peer sender = new Peer(senderName, ((InetSocketAddress)ctx.channel().remoteAddress()).getAddress().getHostAddress(), 0);
                        listener.onReceiveStarted(taskId, fileName, fileSize, sender);

                        // Switch pipeline to file receiving mode
                        metadataReceived = true;
                        ctx.pipeline().remove(LengthFieldBasedFrameDecoder.class);
                        ctx.pipeline().addLast(new FileWriteHandler(listener, taskId, fileName, fileSize));
                        ctx.pipeline().remove(this);
                    }
                } finally {
                    buf.release();
                }
            } else {
                super.channelRead(ctx, msg);
            }
        }
        
        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            cause.printStackTrace();
            ctx.close();
        }
    }

    private static class FileWriteHandler extends ChannelInboundHandlerAdapter {
        private final FileTransferListener listener;
        private final String taskId;
        private final long fileSize;
        private final String savePath;
        
        private FileChannel fileChannel;
        private long bytesReceived = 0;

        public FileWriteHandler(FileTransferListener listener, String taskId, String fileName, long fileSize) throws IOException {
            this.listener = listener;
            this.taskId = taskId;
            this.fileSize = fileSize;
            // Save in current directory with prefix to avoid collision
            this.savePath = "./downloaded_" + fileName; 
            this.fileChannel = new FileOutputStream(savePath).getChannel();
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            ByteBuf buf = (ByteBuf) msg;
            try {
                int readable = buf.readableBytes();
                buf.readBytes(fileChannel, readable);
                bytesReceived += readable;
                listener.onReceiveProgress(taskId, bytesReceived, fileSize);
            } finally {
                buf.release();
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            if (fileChannel != null) {
                fileChannel.close();
            }
            if (bytesReceived >= fileSize) {
                listener.onReceiveCompleted(taskId, savePath);
            } else {
                listener.onReceiveFailed(taskId, new IOException("Connection closed before fully receiving file. Expected: " + fileSize + ", Received: " + bytesReceived));
            }
            super.channelInactive(ctx);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            listener.onReceiveFailed(taskId, cause);
            ctx.close();
        }
    }
}
