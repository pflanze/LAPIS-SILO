#ifndef SILO_REQUEST_HANDLER_H
#define SILO_REQUEST_HANDLER_H

#include <memory>

#include <Poco/Net/HTTPRequestHandler.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>

namespace silo_api {
class LoggingRequestHandler : public Poco::Net::HTTPRequestHandler {
  private:
   std::unique_ptr<Poco::Net::HTTPRequestHandler> wrapped_handler;

  public:
   explicit LoggingRequestHandler(Poco::Net::HTTPRequestHandler* wrapped_handler);

   void handleRequest(
      Poco::Net::HTTPServerRequest& request,
      Poco::Net::HTTPServerResponse& response
   ) override;
};
}  // namespace silo_api

#endif  // SILO_REQUEST_HANDLER_H
