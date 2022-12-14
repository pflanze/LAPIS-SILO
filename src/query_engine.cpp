

#include "silo/query_engine.h"
#include "rapidjson/document.h"
#include "tbb/parallel_for.h"
#include "tbb/parallel_for_each.h"

namespace silo {

struct QueryParseException : public std::exception {
   private:
   const char* message;

   public:
   explicit QueryParseException(const std::string& msg) : message(msg.c_str()) {}

   [[nodiscard]] const char* what() const noexcept override {
      return message;
   }
};

struct BoolExpression {
   // For future, maybe different (return) types of expressions?
   // TypeV type;

   /// Constructor
   explicit BoolExpression(const rapidjson::Value& /*js*/) {}

   /// Destructor
   virtual ~BoolExpression() = default;

   /// Evaluate the expression by interpreting it.
   virtual Roaring* evaluate(const Database& /*db*/, const DatabasePartition& /*dbp*/) = 0;

   /* Maybe generate code in the future
      /// Build the expression LLVM IR code.
      /// @args: all function arguments that can be referenced by an @Argument
      virtual llvm::Value *build(llvm::IRBuilder<> &builder, llvm::Value *args);*/
};

std::unique_ptr<BoolExpression> to_ex(const Database& db, const rapidjson::Value& js);

struct VectorEx : public BoolExpression {
   std::vector<std::unique_ptr<BoolExpression>> children;

   explicit VectorEx(const Database& db, const rapidjson::Value& js) : BoolExpression(js) {
      assert(js.HasMember("children"));
      assert(js["children"].IsArray());
      std::transform(js["children"].GetArray().begin(), js["children"].GetArray().end(),
                     std::back_inserter(children), [&](const rapidjson::Value& js) { return to_ex(db, js); });
   }
};

struct AndEx : public VectorEx {
   explicit AndEx(const Database& db, const rapidjson::Value& js) : VectorEx(db, js) {}

   Roaring* evaluate(const Database& db, const DatabasePartition& dbp) override;
};

struct OrEx : public VectorEx {
   explicit OrEx(const Database& db, const rapidjson::Value& js) : VectorEx(db, js) {}

   Roaring* evaluate(const Database& db, const DatabasePartition& dbp) override;
};

struct NOfEx : public VectorEx {
   unsigned n;
   unsigned impl;
   bool exactly;

   explicit NOfEx(const Database& db, const rapidjson::Value& js) : VectorEx(db, js) {
      n = js["n"].GetUint();
      exactly = js["exactly"].GetBool();
      if (js.HasMember("impl")) {
         impl = js["impl"].GetUint();
      } else {
         impl = 0;
      }
   }

   Roaring* evaluate(const Database& db, const DatabasePartition& dbp) override;
};

struct NegEx : public BoolExpression {
   std::unique_ptr<BoolExpression> child;

   explicit NegEx(const Database& db, const rapidjson::Value& js) : BoolExpression(js) {
      child = to_ex(db, js["child"]);
   }

   Roaring* evaluate(const Database& db, const DatabasePartition& dbp) override;
};

struct DateBetwEx : public BoolExpression {
   time_t from;
   bool open_from;
   time_t to;
   bool open_to;

   explicit DateBetwEx(const Database& /*db*/, const rapidjson::Value& js) : BoolExpression(js) {
      if (js["from"].IsNull()) {
         open_from = true;
      } else {
         open_from = false;

         struct std::tm tm {};
         std::istringstream ss(js["from"].GetString());
         ss >> std::get_time(&tm, "%Y-%m-%d");
         from = mktime(&tm);
      }

      if (js["to"].IsNull()) {
         open_to = true;
      } else {
         open_to = false;

         struct std::tm tm {};
         std::istringstream ss(js["to"].GetString());
         ss >> std::get_time(&tm, "%Y-%m-%d");
         to = mktime(&tm);
      }
   }

   Roaring* evaluate(const Database& db, const DatabasePartition& dbp) override;
};

struct NucEqEx : public BoolExpression {
   unsigned position;
   Symbol value;

   explicit NucEqEx(const Database& /*db*/, const rapidjson::Value& js) : BoolExpression(js) {
      position = js["position"].GetUint();
      value = to_symbol(js["value"].GetString()[0]);
   }

   Roaring* evaluate(const Database& db, const DatabasePartition& dbp) override;
};

struct NucMbEx : public BoolExpression {
   unsigned position;
   Symbol value;

   explicit NucMbEx(const Database& /*db*/, const rapidjson::Value& js) : BoolExpression(js) {
      position = js["position"].GetUint();
      value = to_symbol(js["value"].GetString()[0]);
   }

   Roaring* evaluate(const Database& db, const DatabasePartition& dbp) override;
};

struct NucMutEx : public BoolExpression {
   unsigned reference; // Multiple reference genomes in the future. This indicates, which is queried against.
   unsigned position;

   explicit NucMutEx(const Database& /*db*/, const rapidjson::Value& js) : BoolExpression(js) {
      reference = js.HasMember("reference") && js["reference"].IsUint() ? js["reference"].GetUint() : 0;
      position = js["position"].GetUint();
   }

   Roaring* evaluate(const Database& db, const DatabasePartition& dbp) override;
};

struct PangoLineageEx : public BoolExpression {
   uint32_t lineageKey;
   bool includeSubLineages;

   explicit PangoLineageEx(const Database& db, const rapidjson::Value& js) : BoolExpression(js) {
      includeSubLineages = js["includeSubLineages"].GetBool();
      lineageKey = db.dict->get_pangoid(js["value"].GetString());
   }

   Roaring* evaluate(const Database& db, const DatabasePartition& dbp) override;
};

struct CountryEx : public BoolExpression {
   uint32_t countryKey;

   explicit CountryEx(const Database& db, const rapidjson::Value& js) : BoolExpression(js) {
      countryKey = db.dict->get_countryid(js["value"].GetString());
   }

   Roaring* evaluate(const Database& db, const DatabasePartition& dbp) override;
};

struct RegionEx : public BoolExpression {
   uint32_t regionKey;

   explicit RegionEx(const Database& db, const rapidjson::Value& js) : BoolExpression(js) {
      regionKey = db.dict->get_regionid(js["value"].GetString());
   }

   Roaring* evaluate(const Database& db, const DatabasePartition& dbp) override;
};

struct StrEqEx : public BoolExpression {
   std::string column;
   std::string value;

   explicit StrEqEx(const Database& /*db*/, const rapidjson::Value& js) : BoolExpression(js) {
      column = js["column"].GetString();
      value = js["value"].GetString();
   }

   Roaring* evaluate(const Database& db, const DatabasePartition& dbp) override;
};

std::unique_ptr<BoolExpression> to_ex(const Database& db, const rapidjson::Value& js) {
   assert(js.HasMember("type"));
   assert(js["type"].IsString());
   std::string type = js["type"].GetString();
   if (type == "And") {
      return std::make_unique<AndEx>(db, js);
   } else if (type == "Or") {
      return std::make_unique<OrEx>(db, js);
   } else if (type == "N-Of") {
      return std::make_unique<NOfEx>(db, js);
   } else if (type == "Neg") {
      return std::make_unique<NegEx>(db, js);
   } else if (type == "DateBetw") {
      return std::make_unique<DateBetwEx>(db, js);
   } else if (type == "NucEq") {
      return std::make_unique<NucEqEx>(db, js);
   } else if (type == "NucMut") {
      return std::make_unique<NucMutEx>(db, js);
   } else if (type == "PangoLineage") {
      return std::make_unique<PangoLineageEx>(db, js);
   } else if (type == "StrEq") {
      const std::string& col = js["column"].GetString();
      if (col == "country") {
         return std::make_unique<CountryEx>(db, js);
      } else if (col == "region") {
         return std::make_unique<RegionEx>(db, js);
      } else {
         return std::make_unique<StrEqEx>(db, js);
      }
   } else {
      throw QueryParseException("Unknown object type");
   }
}

Roaring* AndEx::evaluate(const Database& db, const DatabasePartition& dbp) {
   auto ret = children[0]->evaluate(db, dbp);
   for (auto& child : children) {
      auto bm = child->evaluate(db, dbp);
      *ret &= *bm;
      delete bm;
   }
   return ret;
}

Roaring* OrEx::evaluate(const Database& db, const DatabasePartition& dbp) {
   unsigned n = children.size();
   const Roaring* child_res[n];
   for (unsigned i = 0; i < n; i++) {
      child_res[i] = children[i]->evaluate(db, dbp);
   }
   auto ret = new Roaring(Roaring::fastunion(children.size(), child_res));
   for (unsigned i = 0; i < n; i++) {
      delete child_res[i];
   }
   return ret;
}

void vec_and_not(std::vector<uint32_t>& dest, std::vector<uint32_t> v1, std::vector<uint32_t> v2) {
   std::set_difference(v1.begin(), v1.end(), v2.begin(), v2.end(), std::back_inserter(dest));
}

Roaring* NOfExevaluateImpl0(const NOfEx* self, const Database& db, const DatabasePartition& dbp) {
   if (self->exactly) {
      std::vector<uint16_t> count;
      std::vector<uint32_t> at_least;
      std::vector<uint32_t> too_much;
      count.resize(dbp.sequenceCount);
      for (auto& child : self->children) {
         auto bm = child->evaluate(db, dbp);
         for (uint32_t id : *bm) {
            ++count[id];
            if (count[id] == self->n + 1) {
               too_much.push_back(id);
            } else if (count[id] == self->n) {
               at_least.push_back(id);
            }
         }
         delete bm;
      }
      std::vector<uint32_t> correct;
      vec_and_not(correct, at_least, too_much);
      return new Roaring(correct.size(), &correct[0]);

   } else {
      std::vector<uint16_t> count;
      std::vector<uint32_t> correct;
      count.resize(dbp.sequenceCount);
      for (auto& child : self->children) {
         auto bm = child->evaluate(db, dbp);
         for (uint32_t id : *bm) {
            if (++count[id] == self->n) {
               correct.push_back(id);
            }
         }
         delete bm;
      }
      return new Roaring(correct.size(), &correct[0]);
   }
}

Roaring* NOfEx::evaluate(const Database& db, const DatabasePartition& dbp) {
   switch (impl) {
      case 0:
      default:
         return NOfExevaluateImpl0(this, db, dbp);
   }
}

Roaring* NegEx::evaluate(const Database& db, const DatabasePartition& dbp) {
   auto ret = child->evaluate(db, dbp);
   ret->flip(0, dbp.sequenceCount);
   return ret;
}

Roaring* DateBetwEx::evaluate(const Database& /*db*/, const DatabasePartition& dbp) {
   if (open_from && open_to) {
      auto ret = new Roaring;
      ret->addRange(0, dbp.sequenceCount);
      return ret;
   }

   auto ret = new Roaring;
   auto base = &dbp.meta_store.sid_to_date[0];
   for (const chunk_t& chunk : dbp.get_chunks()) {
      auto begin = &dbp.meta_store.sid_to_date[chunk.offset];
      auto end = &dbp.meta_store.sid_to_date[chunk.offset + chunk.count];
      uint32_t lower = open_to ? begin - base : std::lower_bound(begin, end, this->from) - base;
      uint32_t upper = open_to ? end - base : std::upper_bound(begin, end, this->to) - base;
      ret->addRange(lower, upper);
   }
   return ret;
}

Roaring* NucEqEx::evaluate(const Database& /*db*/, const DatabasePartition& dbp) {
   return new Roaring(*dbp.seq_store.bm(position, to_symbol(value)));
}

Roaring* NucMbEx::evaluate(const Database& /*db*/, const DatabasePartition& dbp) {
   return dbp.seq_store.bma(this->position, this->value);
}

Roaring* NucMutEx::evaluate(const Database& db, const DatabasePartition& dbp) {
   char symbol = db.global_reference[this->reference].at(this->position - 1);
   return dbp.seq_store.bma(this->position, to_symbol(symbol));
}

Roaring* PangoLineageEx::evaluate(const Database& /*db*/, const DatabasePartition& dbp) {
   if (includeSubLineages) {
      return new Roaring(dbp.meta_store.sublineage_bitmaps[lineageKey]);
   } else {
      return new Roaring(dbp.meta_store.lineage_bitmaps[lineageKey]);
   }
}

Roaring* CountryEx::evaluate(const Database& /*db*/, const DatabasePartition& dbp) {
   return new Roaring(dbp.meta_store.country_bitmaps[countryKey]);
}

Roaring* RegionEx::evaluate(const Database& /*db*/, const DatabasePartition& dbp) {
   return new Roaring(dbp.meta_store.region_bitmaps[regionKey]);
}

Roaring* StrEqEx::evaluate(const Database& db, const DatabasePartition& dbp) {
   unsigned columnIndex = db.dict->get_colid(this->column);
   constexpr unsigned BUFFER_SIZE = 1024;
   std::vector<uint32_t> buffer(BUFFER_SIZE);
   Roaring* ret = new Roaring;
   for (uint32_t seq : dbp.meta_store.cols[columnIndex]) {
      buffer.push_back(seq);
      if (buffer.size() == BUFFER_SIZE) {
         ret->addMany(BUFFER_SIZE, buffer.data());
         buffer.clear();
      }
   }
   if (buffer.size() > 0) {
      ret->addMany(buffer.size(), buffer.data());
   }
   return ret;
}

} // namespace silo;

std::string execute_query_part(const silo::Database& db, const silo::DatabasePartition& dbp, const std::string& query) {
   using namespace silo;
   rapidjson::Document doc;
   doc.Parse(query.c_str());
   if (!doc.HasMember("filter") || !doc["filter"].IsObject() ||
       !doc.HasMember("action") || !doc["action"].IsObject()) {
      throw QueryParseException("Query json must contain filter and action.");
   }

   std::unique_ptr<BoolExpression> filter = to_ex(db, doc["filter"]);
   // std::string action = doc["action"];
   Roaring* result = filter->evaluate(db, dbp);
   std::stringstream ret;
   ret << "{\"count\":" << result->cardinality() << "}";
   delete result;
   return ret.str();
}

uint64_t execute_count(const silo::Database& db, std::unique_ptr<silo::BoolExpression> ex) {
   std::atomic<uint32_t> count = 0;
   tbb::parallel_for_each(db.partitions.begin(), db.partitions.end(), [&](const auto& dbp) {
      count += ex->evaluate(db, dbp)->cardinality();
   });
   return count;
}

std::string silo::execute_query(const silo::Database& db, const std::string& query) {
   rapidjson::Document doc;
   doc.Parse(query.c_str());
   if (!doc.HasMember("filter") || !doc["filter"].IsObject() ||
       !doc.HasMember("action") || !doc["action"].IsObject()) {
      throw QueryParseException("Query json must contain filter and action.");
   }

   std::unique_ptr<BoolExpression> filter = to_ex(db, doc["filter"]);
   const auto& action = doc["action"];
   const auto& action_type = action["type"].GetString();

   if (action.HasMember("groupByFields")) {
      std::vector<std::string> groupByFields;
      for (const auto& it : action["groupBy"].GetArray()) {
         groupByFields.push_back(it.GetString());
      }
      if (strcmp(action_type, "Aggregated")) {
      } else if (strcmp(action_type, "List")) {
      } else if (strcmp(action_type, "Mutations")) {
      }
   } else {
      if (strcmp(action_type, "Aggregated")) {
         unsigned count = execute_count(db, std::move(filter));
         return "count: " + std::to_string(count);
      } else if (strcmp(action_type, "List")) {
      } else if (strcmp(action_type, "Mutations")) {
      }
   }

   return "Not implemented.";
}
