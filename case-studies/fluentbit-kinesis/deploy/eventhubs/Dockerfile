# Mirrors aws2azure's own Kinesis integration-test emulator image
# (tests/Aws2Azure.IntegrationTests/Kinesis/Emulator/Dockerfile in the main
# repo): the official Azure Event Hubs emulator with a world-readable
# Config.json baked in (its default permissions cause the emulator's
# entrypoint to reject the file otherwise).
FROM alpine:3.19 AS perms
COPY Config.json /staging/Config.json
RUN chmod 0644 /staging/Config.json

FROM mcr.microsoft.com/azure-messaging/eventhubs-emulator:latest
COPY --from=perms /staging/Config.json /Eventhubs_Emulator/ConfigFiles/Config.json
