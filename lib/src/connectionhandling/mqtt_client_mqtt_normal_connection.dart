/*
 * Package : mqtt_client
 * Author : S. Hamblett <steve.hamblett@linux.com>
 * Date   : 22/06/2017
 * Copyright :  S.Hamblett
 */

part of mqtt_server_client;

/// The MQTT normal(insecure TCP) connection class
class MqttNormalConnection extends MqttConnection {
  /// Default constructor
  MqttNormalConnection(events.EventBus eventBus) : super(eventBus);

  /// Initializes a new instance of the MqttConnection class.
  MqttNormalConnection.fromConnect(
      String server, int port, events.EventBus eventBus)
      : super(eventBus) {
    connect(server, port);
  }

  /// Connect - overridden
  @override
  Future<MqttClientConnectionStatus> connect(String server, int port) async {
    try {
     final connectionTask = await Socket.startConnect(server, port);
     client = await connectionTask.socket;
     readWrapper = ReadWrapper();
     messageStream = MqttByteBuffer(typed.Uint8Buffer());
     _startListening();
     return null;
    } on Exception catch (e) {
      _onError(e);
      final message =
          'MqttNormalConnection::The connection to the message broker {$server}:{$port} could not be made.';
      throw NoConnectionException(message);
    }
  }

  @override
  void _disconnect() {
    if (client != null) {
      client.destroy();
      client = null;
    }
  }
}
