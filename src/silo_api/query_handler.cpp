#include "silo_api/query_handler.h"

#include <string>

#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/StreamCopier.h>
#include <spdlog/spdlog.h>
#include <nlohmann/json.hpp>

#include "silo/query_engine/query_engine.h"
#include "silo/query_engine/query_parse_exception.h"
#include "silo/query_engine/query_result.h"
#include "silo_api/error_request_handler.h"

namespace silo_api {

QueryHandler::QueryHandler(
   const silo::query_engine::QueryEngine& query_engine,
   std::string data_version
)
    : query_engine(query_engine),
      data_version(std::move(data_version)) {}

void QueryHandler::post(
   Poco::Net::HTTPServerRequest& request,
   Poco::Net::HTTPServerResponse& response
) {
   std::string query;
   std::istream& istream = request.stream();
   Poco::StreamCopier::copyToString(istream, query);

   SPDLOG_INFO("received query: {}", query);

   response.setContentType("application/json");
   try {
      const auto query_result = query_engine.executeQuery(query);

      response.set("data-version", data_version);

      std::ostream& out_stream = response.send();
      out_stream << nlohmann::json(query_result);
   } catch (const silo::QueryParseException& ex) {
      response.setStatus(Poco::Net::HTTPResponse::HTTP_BAD_REQUEST);
      std::ostream& out_stream = response.send();
      out_stream << nlohmann::json(ErrorResponse{"Bad request", ex.what()});
   }
}

}  // namespace silo_api