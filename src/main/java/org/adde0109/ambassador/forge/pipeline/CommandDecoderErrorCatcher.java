package org.adde0109.ambassador.forge.pipeline;

import com.velocitypowered.api.network.ProtocolVersion;
import com.velocitypowered.api.proxy.server.RegisteredServer;
import com.velocitypowered.proxy.connection.client.ConnectedPlayer;
import com.velocitypowered.proxy.protocol.MinecraftPacket;
import com.velocitypowered.proxy.protocol.ProtocolUtils;
import com.velocitypowered.proxy.protocol.StateRegistry;
import com.velocitypowered.proxy.protocol.packet.AvailableCommandsPacket;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import net.kyori.adventure.text.Component;
import net.kyori.adventure.text.format.NamedTextColor;
import org.adde0109.ambassador.Ambassador;
import org.jetbrains.annotations.NotNull;

public class CommandDecoderErrorCatcher extends ChannelInboundHandlerAdapter {

  private final StateRegistry.PacketRegistry.ProtocolRegistry registry;

  private final ConnectedPlayer player;
  private boolean sentWarning = false;

  public CommandDecoderErrorCatcher(ProtocolVersion protocolVersion, ConnectedPlayer player) {
    this.registry = StateRegistry.PLAY.getProtocolRegistry(ProtocolUtils.Direction.CLIENTBOUND, protocolVersion);
    this.player = player;
  }

  @Override
  public void channelRead(@NotNull ChannelHandlerContext ctx, @NotNull Object msg) throws Exception {
    if (!(msg instanceof ByteBuf buf)) {
      ctx.fireChannelRead(msg);
      return;
    }

    if (!ctx.channel().isActive() || !buf.isReadable()) {
      buf.release();
      return;
    }

    int originalReaderIndex = buf.readerIndex();
    int packetId = ProtocolUtils.readVarInt(buf);
    MinecraftPacket packet = registry.createPacket(packetId);
    buf.readerIndex(originalReaderIndex);

    if (!(packet instanceof AvailableCommandsPacket)) {
      ctx.fireChannelRead(msg);
      return;
    }

    try {
      ByteBuf copy = buf.retainedDuplicate();
      try {
        ProtocolUtils.readVarInt(copy);
        packet.decode(copy, ProtocolUtils.Direction.CLIENTBOUND, registry.version);
      } finally {
        copy.release();
      }

      ctx.fireChannelRead(msg);
    } catch (RuntimeException e) {
      buf.release();
      warnUnsupportedCommands();
    }
  }

  private void warnUnsupportedCommands() {
    if (!Ambassador.getInstance().config.isSilenceWarnings() && !sentWarning) {
      String serverName = "unknown";
      if (player.getConnectedServer() != null) {
        RegisteredServer server = player.getConnectedServer().getServer();
        serverName = server.getServerInfo().getName();
      }

      player.sendMessage(Component.text("[Ambassador Warning]: Unsupported command argument type detected from server "
              + serverName + "! Please install Proxy-Compatible-Forge mod on this backend server "
              + "to have access to commands. This message can be silenced in the ambassador.toml config file.",
          NamedTextColor.YELLOW));
      sentWarning = true;
    }
  }
}
