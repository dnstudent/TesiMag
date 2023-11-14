#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/dataset/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>

#include <memory>
#include <iostream>
#include <filesystem>
namespace fs = std::filesystem;

#define SCIA_PATH fs::path("/Users/davidenicoli/Local_Workspace/Datasets/SCIA")
#define TMIN_PATH fs::path(SCIA_PATH / "giornaliere/minime/db/series.parquet")
#define TMAX_PATH fs::path(SCIA_PATH / "giornaliere/massime/db/series.parquet")

arrow::Result<std::shared_ptr<arrow::Table>> ReadFile(const fs::path &path_to_file)
{
    arrow::MemoryPool *pool = arrow::default_memory_pool();
    std::shared_ptr<arrow::io::RandomAccessFile> input;
    ARROW_ASSIGN_OR_RAISE(input, arrow::io::ReadableFile::Open(path_to_file));

    // Open Parquet file reader
    std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
    ARROW_RETURN_NOT_OK(parquet::arrow::OpenFile(input, pool, &arrow_reader));

    // Read entire file as a single Arrow table
    std::shared_ptr<arrow::Table> table;
    ARROW_RETURN_NOT_OK(arrow_reader->ReadTable(&table));

    return table;
}

arrow::Status RunMain()
{
    ARROW_ASSIGN_OR_RAISE(const auto lfs, arrow::fs::FileSystemFromUriOrPath(SCIA_PATH));
    arrow::fs::FileSelector file_selector;
    file_selector.base_dir = SCIA_PATH;
    arrow::dataset::FileSystemFactoryOptions options;
    options.partitioning = arrow::dataset::HivePartitioning::MakeFactory();
    const auto read_format = std::make_shared<arrow::dataset::ParquetFileFormat>();
    ARROW_ASSIGN_OR_RAISE(const auto factory, arrow::dataset::FileSystemDatasetFactory::Make(lfs, {TMIN_PATH, TMAX_PATH}, read_format, options));
    ARROW_ASSIGN_OR_RAISE(const auto dataset, factory->Finish());
    ARROW_ASSIGN_OR_RAISE(auto fragments, dataset->GetFragments());
    for (const auto &frag : fragments)
    {
        std::cout << "Frag: " << (*frag)->ToString() << '\n';
        std::cout << "Partition expression: " << (*frag)->partition_expression().ToString() << '\n';
    }
    ARROW_ASSIGN_OR_RAISE(const auto scan_builder, dataset->NewScan());
    ARROW_ASSIGN_OR_RAISE(const auto scanner, scan_builder->Finish());
    ARROW_ASSIGN_OR_RAISE(const auto table, scanner->ToTable());

    std::cout << table->Slice(0, 10)->ToString() << std::endl;

    // ARROW_ASSIGN_OR_RAISE(auto tmin, ReadFile(TMIN_PATH));
    // ARROW_ASSIGN_OR_RAISE(auto tmax, ReadFile(TMAX_PATH));
    // std::cout << tmin->Slice(0, 10)->ToString() << std::endl;
    return arrow::Status::OK();
}

int main()
{
    arrow::Status st = RunMain();
    if (!st.ok())
    {
        std::cerr << st << std::endl;
        return 1;
    }
    return 0;
}