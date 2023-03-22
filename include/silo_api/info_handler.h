#ifndef SILO_INFOHANDLER_H
#define SILO_INFOHANDLER_H

#include <Poco/Net/HTTPRequestHandler.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>

namespace silo {
class Database;
}

namespace silo_api {
class InfoHandler : public Poco::Net::HTTPRequestHandler {
  private:
   const silo::Database& database;

  public:
   explicit InfoHandler(const silo::Database& database);

   void handleRequest(
      Poco::Net::HTTPServerRequest& request,
      Poco::Net::HTTPServerResponse& response
   ) override;
};
}  // namespace silo_api

#endif  // SILO_INFOHANDLER_H
