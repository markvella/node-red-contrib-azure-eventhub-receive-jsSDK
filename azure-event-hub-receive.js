const { EventHubConsumerClient, latestEventPosition } = require("@azure/event-hubs");

module.exports = function (RED) {
    function AzureEventHubReceiveNode(config) {
        RED.nodes.createNode(this, config);

        var node = this;
        node.connectionstring = config.connectionstring;
        node.eventhubname = config.eventhubname;
        node.consumergroup = config.consumergroup;

        // clear status, might be left over after updating settings
        node.status({});

        try {
            const consumerClient = new EventHubConsumerClient(config.consumergroup, config.connectionstring, config.eventhubname);

            const subscription = consumerClient.subscribe(
                {
                  // The callback where you add your code to process incoming events
                  processEvents: async (events, context) => {
                    // message received from Event Hub partition
                    for (const event of events) {
                        console.log(`Received event from partition: '${context.partitionId}' and consumer group: '${context.consumerGroup}'`);
                        var msg = { payload: event.body }
                        node.send(msg);
                    }
                  },
                  processError: async (err, context) => {
                    node.status({ fill: "yellow", shape: "ring", text: "error received, see debug or output" });
                    node.error(err.message);
                  },
                  processClose: async (err, context) => {
                    node.log('closing ...' + context.CloseReason);
                  }
                }, 
                { startPosition: latestEventPosition }
              );
        }
        catch (err) {
            this.error(err.message);
            node.status({ fill: "red", shape: "ring", text: "can't connect, " + err.message });
        }

        this.on('close', function (done) {
          node.log('closing ...');
          subscription.close();
          consumerClient.close();
          node.log('closing done.');
          });

    }

    RED.nodes.registerType("azure-event-hub-receive", AzureEventHubReceiveNode);
}

