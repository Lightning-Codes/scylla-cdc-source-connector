package com.scylladb.cdc.debezium.connector;

import com.datastax.driver.core.EndPoint;
import com.datastax.driver.core.RemoteEndpointAwareNettySSLOptions;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;

/** TLS options that require every initial and discovered endpoint to match a certificate SAN. */
final class StrictRemoteEndpointAwareNettySSLOptions extends RemoteEndpointAwareNettySSLOptions {
  static final String ENDPOINT_IDENTIFICATION_ALGORITHM = "HTTPS";

  StrictRemoteEndpointAwareNettySSLOptions(SslContext context) {
    super(context);
  }

  @Override
  public SslHandler newSSLHandler(SocketChannel channel, EndPoint remoteEndpoint) {
    SslHandler handler = super.newSSLHandler(channel, remoteEndpoint);
    enableEndpointIdentification(handler.engine());
    return handler;
  }

  static void enableEndpointIdentification(SSLEngine engine) {
    SSLParameters parameters = engine.getSSLParameters();
    parameters.setEndpointIdentificationAlgorithm(ENDPOINT_IDENTIFICATION_ALGORITHM);
    engine.setSSLParameters(parameters);
  }
}
