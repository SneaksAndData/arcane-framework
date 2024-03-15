FROM mcr.microsoft.com/dotnet/sdk:6.0 AS build-env
ARG NUGET_TOKEN
ARG PROJECT_NAME

WORKDIR /app

# Copy csproj and restore as distinct layers
COPY src/*.csproj ./
RUN dotnet nuget add source --username USERNAME --password $NUGET_TOKEN --store-password-in-clear-text --name github "https://nuget.pkg.github.com/SneaksAndData/index.json"
RUN dotnet restore

# Copy everything else and build
COPY src/. ./
RUN dotnet publish "$PROJECT_NAME.csproj" -c Release -o out

# Build runtime image
FROM mcr.microsoft.com/dotnet/aspnet:6.0-bullseye-slim
ARG TRACER_VERSION="2.32.0"
ARG PROJECT_NAME
ENV PROJECT_ASSEMBLY=$PROJECT_NAME

RUN apt-get update -y && apt-get install -y curl jq

# Download and install the Datadog Tracer
RUN mkdir -p /opt/datadog \
    && mkdir -p /var/log/datadog \
    && curl -LO https://github.com/DataDog/dd-trace-dotnet/releases/download/v${TRACER_VERSION}/datadog-dotnet-apm_${TRACER_VERSION}_amd64.deb \
    && dpkg -i ./datadog-dotnet-apm_${TRACER_VERSION}_amd64.deb \
    && rm ./datadog-dotnet-apm_${TRACER_VERSION}_amd64.deb 
    

WORKDIR /app
COPY --from=build-env /app/out .
ENTRYPOINT "dotnet" "$PROJECT_ASSEMBLY.dll"
