/*
 * @file PluginManager.cpp
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
*
* @author roman.simakov@gmail.com
*/

#include <dlfcn.h>
#include <boost/foreach.hpp>
#include <log4cxx/logger.h>

#include <array/Chunk.h>

#include <util/PluginManager.h>
#include <system/Exceptions.h>

#ifndef SCIDB_CLIENT
#include <system/Config.h>
#endif

#include <system/SystemCatalog.h>
#include <system/SciDBConfigOptions.h>
#include <query/Aggregate.h>
#include <query/OperatorLibrary.h>

namespace scidb
{

using namespace std;

// to prevent visibility of variable outside of file
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.qproc.pluginmanager"));

PluginManager::PluginManager()
{
//Option CONFIG_PLUGINSDIR is correct only for server
#ifndef SCIDB_CLIENT
    setPluginsDirectory(Config::getInstance()->getOption<string>(CONFIG_PLUGINSDIR));
#endif
}

void PluginManager::preLoadLibraries()
{
    ScopedMutexLock cs (_mutex, PTW_SML_PM);

#ifndef SCIDB_CLIENT
    SystemCatalog* cat = SystemCatalog::getInstance();
    if (cat->isConnected()) {
        vector<string> libraries;
        cat->getLibraries(libraries);
        for (size_t i = 0; i < libraries.size(); i++) {
            try {
                loadLibrary(libraries[i], false);
            }
            catch (const Exception& e) {
                LOG4CXX_WARN(logger, "Error of loading " << libraries[i] << ": " << e.what())
            }
            catch (...) {
                LOG4CXX_WARN(logger, "Unknown error of loading " << libraries[i])
            }
        }
    }
#endif
}

PluginManager::~PluginManager()
{
    BOOST_FOREACH(Plugins::value_type const& i, _plugins)
    {
        dlclose(i.second._handle);
    }
}

typedef void (*GetPluginVersion)(uint32_t&, uint32_t&, uint32_t&, uint32_t&);

PluginManager::Plugin& PluginManager::findModule(const std::string& moduleName, bool* was)
{
    ScopedMutexLock cs (_mutex, PTW_SML_PM);

    if (was)
        *was = true;
    if (_plugins.find(moduleName) != _plugins.end())
        return _plugins[moduleName];

    string fullName = "lib" + moduleName + ".so";
    if (_plugins.find(fullName) != _plugins.end())
        return _plugins[fullName];
    if (was)
        *was = false;
    string path = _pluginsDirectory + "/" + moduleName;
    void* plugin = dlopen(path.c_str(), RTLD_LAZY|RTLD_LOCAL);
    if (!plugin) {
        path = _pluginsDirectory + "/" + fullName;
        plugin = dlopen(path.c_str(), RTLD_LAZY|RTLD_LOCAL);
        if (!plugin) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_PLUGIN_MGR, SCIDB_LE_CANT_LOAD_MODULE) << path << dlerror();
        }
    }
    else {
        fullName = moduleName;
    }
    Plugin pluginDesc(fullName,plugin);

    GetPluginVersion getPluginVersion = reinterpret_cast<GetPluginVersion>(reinterpret_cast<size_t>(openSymbol(plugin, "GetPluginVersion")));
    if (getPluginVersion) {
        getPluginVersion(pluginDesc._major, pluginDesc._minor, pluginDesc._patch, pluginDesc._build);
        if (pluginDesc._major != SCIDB_VERSION_MAJOR() || pluginDesc._minor != SCIDB_VERSION_MINOR()) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_PLUGIN_MGR, SCIDB_LE_WRONG_MODULE_VERSION) << moduleName
                << pluginDesc._major << pluginDesc._minor << pluginDesc._patch << pluginDesc._build
                << SCIDB_VERSION();
        }
        LOG4CXX_INFO(logger, "Version of " << moduleName << " is " << pluginDesc._major <<
                     "." << pluginDesc._minor << "." << pluginDesc._patch << "." << pluginDesc._build)
    } else {
        LOG4CXX_INFO(logger, "Unknown version of library " << moduleName)
    }
    Plugin& res = _plugins[fullName];
    res = pluginDesc;

    return res;
}

void* PluginManager::openSymbol(void* plugin, const std::string& symbolName, bool throwException)
{
    void* symbol = dlsym(plugin, symbolName.c_str());
    if (!symbol && throwException) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_PLUGIN_MGR, SCIDB_LE_CANT_FIND_SYMBOL) << symbolName << dlerror();
    }
    return symbol;
}

typedef const vector<BaseLogicalOperatorFactory*>&  (*GetLogicalOperatorFactories)();
typedef const vector<BasePhysicalOperatorFactory*>& (*GetPhysicalOperatorFactories)();
typedef const vector<Type>&                         (*GetTypes)();
typedef const vector<FunctionDescription>&          (*GetFunctions)();
typedef const vector<AggregatePtr>&                 (*GetAggregates)();

template<typename T>
class Eraser
{
public:
    Eraser(T& value): _value(value), _ref(value) {
    }

    ~Eraser() {
        _ref = _value;
    }

private:
    T _value;
    T& _ref;
};

/**
 * Loading includes several parts:
 * 1) Loading library itself.
 * 2) Call function and get a version of plugin
 * 3) Call function and get vector of user defined types for adding into typesystem
 * 4) Call function and get a vector of logical operator factories for adding into OperatorLibrary
 * 5) Call function and get a vector of physical operator factories for adding into OperatorLibrary
 * 6) Call function and get a vector of aggregate pointers for adding into AggregateLibrary
 * 7) Call function and get a vector of user defined scalar function for adding into FunctionLibrary
 */
void PluginManager::loadLibrary(const string& libraryName, bool registerInCatalog)
{
    ScopedMutexLock cs (_mutex, PTW_SML_PM);

    Eraser<std::string> eraser(_loadingLibrary);
    _loadingLibrary = libraryName;
    bool was;
    Plugin& pluginDesc = findModule(libraryName, &was);
    void* library = pluginDesc._handle;
    if (!was) {

        GetTypes getTypes = reinterpret_cast<GetTypes>(reinterpret_cast<size_t>(openSymbol(library, "GetTypes")));
        if (getTypes) {
            const vector<Type>& types = getTypes();
            for (size_t i = 0; i < types.size(); i++) {
                TypeLibrary::registerType(types[i]);
            }
        }

#ifndef SCIDB_CLIENT
        GetLogicalOperatorFactories getLogicalOperatorFactories = reinterpret_cast<GetLogicalOperatorFactories>(reinterpret_cast<size_t>(openSymbol(library, "GetLogicalOperatorFactories")));
        if (getLogicalOperatorFactories) {
            const vector<BaseLogicalOperatorFactory*>& logicalOperatorFactories = getLogicalOperatorFactories();
            for (size_t i = 0; i < logicalOperatorFactories.size(); i++) {
                OperatorLibrary::getInstance()->addLogicalOperatorFactory(logicalOperatorFactories[i]);
            }
        }

        GetPhysicalOperatorFactories getPhysicalOperatorFactories = reinterpret_cast<GetPhysicalOperatorFactories>(reinterpret_cast<size_t>(openSymbol(library, "GetPhysicalOperatorFactories")));
        if (getPhysicalOperatorFactories) {
            const vector<BasePhysicalOperatorFactory*>& physicalOperatorFactories = getPhysicalOperatorFactories();
            for (size_t i = 0; i < physicalOperatorFactories.size(); i++) {
                OperatorLibrary::getInstance()->addPhysicalOperatorFactory(physicalOperatorFactories[i]);
            }
        }

        GetAggregates getAggregates = reinterpret_cast<GetAggregates>(reinterpret_cast<size_t>(openSymbol(library, "GetAggregates")));
        if (getAggregates)
        {
            const vector< AggregatePtr>& aggregates = getAggregates();
            for (size_t i = 0; i < aggregates.size(); i++) {
                AggregateLibrary::getInstance() -> addAggregate(aggregates[i], libraryName);
            }
        }

#endif
        GetFunctions getFunctions = reinterpret_cast<GetFunctions>(reinterpret_cast<size_t>(openSymbol(library, "GetFunctions")));
        if (getFunctions) {
            vector< FunctionDescription> functions = getFunctions();
            for (size_t i = 0; i < functions.size(); i++) {
                FunctionLibrary::getInstance()->addFunction(functions[i]);
            }
        }
    }
#ifndef SCIDB_CLIENT
    if (registerInCatalog) {
        SystemCatalog::getInstance()->addLibrary(libraryName);
    }
#endif
}

void PluginManager::unLoadLibrary(const string& libraryName)
{
    ScopedMutexLock cs (_mutex, PTW_SML_PM);

    string fullName = "lib" + libraryName + ".so";
    if (_plugins.find(libraryName) == _plugins.end() && _plugins.find(fullName) == _plugins.end())
        throw SYSTEM_EXCEPTION(SCIDB_SE_PLUGIN_MGR, SCIDB_LE_CANT_UNLOAD_MODULE) << libraryName;
#ifndef SCIDB_CLIENT
    SystemCatalog::getInstance()->removeLibrary(libraryName);

    LOG4CXX_WARN(logger, "Unloading '" << libraryName << "' library. Some arrays may be unavailable after server restart");
#endif
}

void PluginManager::setPluginsDirectory(const std::string &pluginsDirectory)
{
    ScopedMutexLock cs (_mutex, PTW_SML_PM);
    _pluginsDirectory = pluginsDirectory;
}

void PluginManager::visitPlugins(const Visitor& visit) const
{
    ScopedMutexLock cs(_mutex, PTW_SML_PM);

    visit(Plugin(
            "SciDB",NULL,
            SCIDB_VERSION_MAJOR(),
            SCIDB_VERSION_MINOR(),
            SCIDB_VERSION_PATCH(),
            SCIDB_VERSION_BUILD(),
            SCIDB_BUILD_TYPE()));

    BOOST_FOREACH (Plugins::value_type const& i,_plugins)
    {
        visit(i.second);
    }
}

PluginManager::Plugin::Plugin(std::string const& name,
                              void*              handle,
                              uint32_t           major,
                              uint32_t           minor,
                              uint32_t           patch,
                              uint32_t           build,
                              std::string const& buildType)
 :  _name(name),
    _handle(handle),
    _major(major),
    _minor(minor),
    _patch(patch),
    _build(build),
    _buildType(buildType)
{}

} // namespace
