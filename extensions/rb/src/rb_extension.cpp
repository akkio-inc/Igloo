//===----------------------------------------------------------------------===//
// rb: exact distinct counting with roaring bitmaps for DuckDB (see extensions/rb/README.md).
//===----------------------------------------------------------------------===//
#define DUCKDB_EXTENSION_MAIN
#include "rb_bitmap.hpp"
#include "rb_memory.hpp"

#include "duckdb/main/config.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

class RbExtension : public Extension {
public:
	void Load(ExtensionLoader &loader) override;
	std::string Name() override {
		return "rb";
	}
	std::string Version() const override {
		return "1";
	}
};

static void CheckMode(ClientContext &, SetScope, Value &parameter) {
	auto mode = StringUtil::Lower(parameter.ToString());
	if (mode != "off" && mode != "always") {
		throw InvalidInputException("rb_count_distinct is 'off' or 'always', not '%s'", parameter.ToString());
	}
	parameter = Value(mode);
}

static void LoadInternal(ExtensionLoader &loader) {
	rb::InstallMemoryHook();
	rb::RegisterAggregates(loader);
	rb::RegisterScalars(loader);
	auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
	config.AddExtensionOption("rb_count_distinct",
	                          "Whether COUNT(DISTINCT) of 32-bit unsigned ids counts into roaring bitmaps: 'off' or "
	                          "'always' (Igloo decides per statement)",
	                          LogicalType::VARCHAR, Value("off"), CheckMode);
	rb::RegisterRewrite(config);
	rb::RegisterStats(loader, config);
	rb::RegisterFold(config);
	rb::RegisterStrings(loader);
	rb::RegisterDivide(loader);
	rb::RegisterFlatten(loader);
	rb::RegisterFileCache(loader);
	config.AddExtensionOption("rb_memory_budget",
	                          "The most memory one COUNT(DISTINCT)'s bitmaps may take (e.g. '2GB'; empty: whatever "
	                          "DuckDB's memory_limit leaves)",
	                          LogicalType::VARCHAR, Value(""));
}

void RbExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

} // namespace duckdb

extern "C" {
DUCKDB_CPP_EXTENSION_ENTRY(rb, loader) {
	duckdb::LoadInternal(loader);
}
}
