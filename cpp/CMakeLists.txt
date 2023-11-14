cmake_minimum_required(VERSION 3.27)

project(CppArrowTools)

find_package(Arrow REQUIRED)
find_package(ArrowDataset REQUIRED)
find_package(Parquet REQUIRED)

add_executable(dataset dataset.cpp)
target_link_libraries(dataset PRIVATE Arrow::arrow_shared Parquet::parquet_shared ArrowDataset::arrow_dataset_shared)
target_compile_features(dataset PUBLIC cxx_std_20)