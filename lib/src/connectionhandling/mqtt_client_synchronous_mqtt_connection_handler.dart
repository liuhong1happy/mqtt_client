/*
 * Package : mqtt_client
 * Author : S. Hamblett <steve.hamblett@linux.com>
 * Date   : 22/06/2017
 * Copyright :  S.Hamblett
 */

part of mqtt_client;

/// Connection handler that performs connections and disconnections to the hostname in a synchronous manner.
class SynchronousMqttConnectionHandler extends MqttConnectionHandler {
  /// Initializes a new instance of the MqttConnectionHandler class.
  SynchronousMqttConnectionHandler(this._clientEventBus);

  /// 超时持续时长
  static Duration timeoutDuration = Duration(seconds: 5);

  /// The event bus
  events.EventBus _clientEventBus;

  StreamSubscription<MessageAvailable> _subscription;

  /// Synchronously connect to the specific Mqtt Connection.
  @override
  Future<MqttClientConnectionStatus> internalConnect(
      String hostname, int port, MqttConnectMessage connectMessage) async {

    MqttLogger.log('SynchronousMqttConnectionHandler::internalConnect entered');
    
    connectionStatus.state = MqttConnectionState.connecting;
    if (useWebSocket) {
      if (useAlternateWebSocketImplementation) {
        MqttLogger.log(
            'SynchronousMqttConnectionHandler::internalConnect - alternate websocket implementation selected');
        connection = MqttWs2Connection(securityContext, _clientEventBus);
      } else {
        MqttLogger.log(
            'SynchronousMqttConnectionHandler::internalConnect - websocket selected');
        connection = MqttWsConnection(_clientEventBus);
      }
      if (websocketProtocols != null) {
        connection.protocols = websocketProtocols;
      }
    } else if (secure) {
      MqttLogger.log(
          'SynchronousMqttConnectionHandler::internalConnect - secure selected');
      connection = MqttSecureConnection(
          securityContext, _clientEventBus, onBadCertificate);
    } else {
      MqttLogger.log(
          'SynchronousMqttConnectionHandler::internalConnect - insecure TCP selected');
      connection = MqttNormalConnection(_clientEventBus);
    }
    connection.onDisconnected = onDisconnected;

    await connection.connect(hostname, port);
    if(_subscription != null && !_subscription.isPaused) {
      await _subscription.cancel();
    }
    _subscription = _clientEventBus.on<MessageAvailable>().listen(messageAvailable);
    // Transmit the required connection message to the broker.
    MqttLogger.log('SynchronousMqttConnectionHandler::internalConnect sending connect message');

    // 带有超时逻辑 20s
    final Completer<MqttClientConnectionStatus> completer = Completer<MqttClientConnectionStatus>();
    final Timer connectTimer = Timer(timeoutDuration, (){
      _performConnectionDisconnect();
      completer.complete();
    });
    registerForMessage(MqttMessageType.connectAck, (MqttMessage msg) => _connectAckProcessor(msg, (){
      connectTimer.cancel();
      completer.complete();
    }));
    sendMessage(connectMessage);
    await completer.future;
    if(connectTimer.isActive) {
      connectTimer.cancel();
    }
    MqttLogger.log('SynchronousMqttConnectionHandler::internalConnect - state = $connectionStatus');
    /// 如果是链接过程中失败，需要清除socket
    if(connectionStatus.state == MqttConnectionState.disconnected) {
      connection.disconnect();
    }

    return connectionStatus;
  }

  /// Disconnects
  @override
  MqttConnectionState disconnect() {
    MqttLogger.log('SynchronousMqttConnectionHandler::disconnect');
    // Send a disconnect message to the broker
    connectionStatus.state = MqttConnectionState.disconnecting;
    sendMessage(MqttDisconnectMessage());
    _performConnectionDisconnect();
    return connectionStatus.state = MqttConnectionState.disconnected;
  }

  /// Disconnects the underlying connection object.
  void _performConnectionDisconnect() {
    connectionStatus.state = MqttConnectionState.disconnected;
  }

  /// Processes the connect acknowledgement message.
  bool _connectAckProcessor(MqttMessage msg,  Function callback) {
    MqttLogger.log('SynchronousMqttConnectionHandler::_connectAckProcessor');
    try {
      final MqttConnectAckMessage ackMsg = msg;
      // Drop the connection if our connect request has been rejected.
      if (ackMsg.variableHeader.returnCode ==
              MqttConnectReturnCode.brokerUnavailable ||
          ackMsg.variableHeader.returnCode ==
              MqttConnectReturnCode.identifierRejected ||
          ackMsg.variableHeader.returnCode ==
              MqttConnectReturnCode.unacceptedProtocolVersion ||
          ackMsg.variableHeader.returnCode ==
              MqttConnectReturnCode.notAuthorized ||
          ackMsg.variableHeader.returnCode ==
              MqttConnectReturnCode.badUsernameOrPassword) {
        MqttLogger.log(
            'SynchronousMqttConnectionHandler::_connectAckProcessor connection rejected');
        connectionStatus.returnCode = ackMsg.variableHeader.returnCode;
        _performConnectionDisconnect();
      } else {
        // Initialize the keepalive to start the ping based keepalive process.
        MqttLogger.log(
            'SynchronousMqttConnectionHandler::_connectAckProcessor - state = connected');
        connectionStatus.state = MqttConnectionState.connected;
        connectionStatus.returnCode = MqttConnectReturnCode.connectionAccepted;
        // Call the connected callback if we have one
        if (onConnected != null) {
          onConnected();
        }
      }
    } on Exception {
      _performConnectionDisconnect();
    }
    callback();
    return true;
  }
}
