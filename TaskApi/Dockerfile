# Build context = repository root (see docker-compose.yml).
FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
WORKDIR /src
COPY TaskApi/TaskApi.csproj TaskApi/
RUN dotnet restore TaskApi/TaskApi.csproj
COPY TaskApi/ TaskApi/
RUN dotnet publish TaskApi/TaskApi.csproj -c Release -o /app --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:9.0
WORKDIR /app
COPY --from=build /app .
ENV ASPNETCORE_URLS=http://+:8080
EXPOSE 8080
ENTRYPOINT ["dotnet", "TaskApi.dll"]
