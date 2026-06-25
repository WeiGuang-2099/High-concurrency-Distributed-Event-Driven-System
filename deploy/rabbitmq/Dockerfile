FROM rabbitmq:3-management

ADD https://github.com/rabbitmq/rabbitmq-delayed-message-exchange/releases/download/v3.13.0/rabbitmq_delayed_message_exchange-3.13.0.ez /opt/rabbitmq/plugins/

# ADD leaves the .ez root-owned with mode 600; the broker runs as the rabbitmq user
# and silently fails to load the plugin (eacces). Make it world-readable so it loads.
RUN chmod 644 /opt/rabbitmq/plugins/rabbitmq_delayed_message_exchange-3.13.0.ez

RUN rabbitmq-plugins enable --offline rabbitmq_delayed_message_exchange
