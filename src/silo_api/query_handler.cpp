#include "silo_api/query_handler.h"

#include <cxxabi.h>
#include <string>

#include <Poco/Net/HTTPResponse.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/StreamCopier.h>
#include <spdlog/spdlog.h>
#include <nlohmann/json.hpp>

#include "silo/query_engine/query_parse_exception.h"
#include "silo_api/database_mutex.h"
#include "silo_api/error_request_handler.h"

namespace silo_api {

QueryHandler::QueryHandler(silo_api::DatabaseMutex& database_mutex)
    : database_mutex(database_mutex) {}

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
      const auto fixed_database = database_mutex.getDatabase();

      const auto query_result = fixed_database.database.executeQuery(query);

      response.set("data-version", fixed_database.database.getDataVersion().toString());

      std::ostream& out_stream = response.send();
      out_stream << nlohmann::json(query_result);
   } catch (const silo::QueryParseException& ex) {
      SPDLOG_INFO("Query is invalid: " + query);
      response.setStatus(Poco::Net::HTTPResponse::HTTP_BAD_REQUEST);
      std::ostream& out_stream = response.send();
      out_stream << nlohmann::json(ErrorResponse{"Bad request", ex.what()});
   } catch (const std::exception& ex) {
      SPDLOG_ERROR(ex.what());
      response.setStatus(Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR);
      std::ostream& out_stream = response.send();
      out_stream << nlohmann::json(ErrorResponse{"Internal Server Error", ex.what()});
   } catch (const std::string& ex) {
      SPDLOG_ERROR(ex);
      response.setStatus(Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR);
      std::ostream& out_stream = response.send();
      out_stream << nlohmann::json(ErrorResponse{"Internal Server Error", ex});
   } catch (...) {
      SPDLOG_ERROR("Query cancelled with uncatchable (...) exception");
      const auto exception = std::current_exception();
      if (exception) {
         const auto* message = abi::__cxa_current_exception_type()->name();
         SPDLOG_ERROR("current_exception: {}", message);
         response.setStatus(Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR);
         std::ostream& out_stream = response.send();
         out_stream << nlohmann::json(ErrorResponse{"Internal Server Error", message});
      } else {
         response.setStatus(Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR);
         std::ostream& out_stream = response.send();
         out_stream << nlohmann::json(
            ErrorResponse{"Internal Server Error", "non recoverable error message"}
         );
      }
   }
}

}  // namespace silo_api