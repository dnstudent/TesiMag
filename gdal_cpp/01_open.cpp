#include <gdal_priv.h>

#include <errno.h>
#include <iostream>

int main(int argc, const char *argv[])
{
    if (argc != 2)
    {
        return EINVAL;
    }
    const char *pszFilename = argv[1];

    GDALDatasetUniquePtr poDataset;
    GDALAllRegister();
    const GDALAccess eAccess = GA_ReadOnly;
    poDataset = GDALDatasetUniquePtr(GDALDataset::FromHandle(GDALOpen(pszFilename, eAccess)));
    if (!poDataset)
    {
        // handle error
        std::cout << "There was an error" << std::endl;
    }
    const auto names = poDataset->GetFieldDomainNames();
    for (const auto &name : names)
    {
        std::cout << name << '\n';
    }

    const auto descr = poDataset->GetProjectionRef();

    std::cout << descr << '\n';

    poDataset->Close();
    return 0;
}