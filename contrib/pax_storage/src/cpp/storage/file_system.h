#pragma once

#include <string>

namespace pax {

class FileSystem {
 public:
    virtual ~FileSystem() = 0;
    virtual bool Open(...) = 0;
    virtual std::string BuildPath(const std::string & file_name) const = 0;

 protected:
};

}  //  namespace pax
