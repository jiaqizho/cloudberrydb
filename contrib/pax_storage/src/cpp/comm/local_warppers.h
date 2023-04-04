#pragma once

struct MemoryContext {};

/* ResourceOwner */
struct ResourceOwner {};
void ResourceOwnerRememberFile(ResourceOwner owner, int file);
void ResourceOwnerForgetFile(ResourceOwner owner, int file);
