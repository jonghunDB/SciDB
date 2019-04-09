/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2008-2018 SciDB, Inc.
* All Rights Reserved.
*
* SciDB is free software: you can redistribute it and/or modify
* it under the terms of the AFFERO GNU General Public License as published by
* the Free Software Foundation.
*
* SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
* INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
* NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
* the AFFERO GNU General Public License for the complete license terms.
*
* You should have received a copy of the AFFERO GNU General Public License
* along with SciDB.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
*
* END_COPYRIGHT
*/

#ifndef ONSCOPEEXIT_H_
#define ONSCOPEEXIT_H_

#include <functional>

namespace scidb
{

/**
 * Class OnScopeExit
 *
 * Invokes the passed callable on destruction.
 * Example usage with lambda calling free-function:
 *   OnScopeExit onExit([] () { callCleanupFunction(); });
 *
 * Example usage with lambda calling member method:
 *   OnScopeExit onExit([this] () { this->callCleanupFunction(); });
 */
class OnScopeExit
{
public:
    OnScopeExit() = delete;
    OnScopeExit(const OnScopeExit&) = delete;
    OnScopeExit(OnScopeExit&&) = default;

    template <typename FunctionT>
    OnScopeExit(FunctionT&& w)
        : _work(std::move(w))
    {
    }

    ~OnScopeExit()
    {
        if (_work) {
            _work();
        }
    }

    void cancel()
    {
        _work = nullptr;
    }

private:
    std::function<void()> _work;
};

}  // namespace scidb

#endif  // ONSCOPEEXIT_H_
