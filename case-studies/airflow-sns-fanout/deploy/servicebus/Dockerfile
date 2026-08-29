# Stage 1 — set world-readable perms without requiring BuildKit.
FROM alpine:3.19 AS perms
COPY Config.json /staging/Config.json
RUN chmod 0644 /staging/Config.json

FROM mcr.microsoft.com/azure-messaging/servicebus-emulator:latest
COPY --from=perms /staging/Config.json /ServiceBus_Emulator/ConfigFiles/Config.json
