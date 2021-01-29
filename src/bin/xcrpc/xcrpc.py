#!/usr/bin/env python3

import sys
import os

from google.protobuf.compiler import plugin_pb2 as plugin

import jinja2


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


def camelCase(raw):
    """formats an array of string components into camelCase"""
    return raw[0][0].lower() + raw[0][1:] + "".join(raw[1:])


def CamelCase(raw):
    """formats an array of string components into CamelCase"""
    return raw[0][0].upper() + raw[0][1:] + "".join(raw[1:])


def pythonPackage(raw):
    package = list(raw[:-1])

    if package[0] == "google":
        # this must be a 'well known type' so the package is weird
        module = raw[-1].lower()
        package.append(module)
    package[-1] = package[-1] + "_pb2"
    return ".".join(package)


def jsPackage(raw):
    raw = list(raw)
    package = raw[:-1]

    if raw[0] == "google":
        raw.insert(0, "google-protobuf")
        # this must be a 'well known type' so the package is weird
        module = raw[-1].lower()
        package.append(module)
    else:
        # this isn't a google package
        raw.insert(0, ".")
    package[-1] = package[-1] + "_pb"
    return "/".join(package)


templateDir = os.path.join(os.environ["XLRDIR"], "src", "bin", "xcrpc")
loader = jinja2.FileSystemLoader(templateDir)
jenv = jinja2.Environment(loader=loader, trim_blocks=True, lstrip_blocks=True)


class CodeGenerator:
    def getTemplateFileName(self):
        raise NotImplementedError()

    def getOutputFileName(self, namespace, filebase):
        raise NotImplementedError()

    def getFileInfo(self):
        raise NotImplementedError()


class CppHGenerator(CodeGenerator):
    def getTemplateFileName(self):
        return "Service.xcrpc.h.j2"

    def getOutputFileName(self, namespace, filebase):
        return os.path.join(namespace, f"{filebase}.xcrpc.h")

    def getFileInfo(self, pFile):
        services = []
        for service in pFile.service:
            methods = []
            for method in service.method:
                inputPackage, inputClass, inputType = _parseType(
                    method.input_type)
                outputPackage, outputClass, outputType = _parseType(
                    method.output_type)
                methods.append({
                    "name": CamelCase([method.name]),
                    "function": camelCase([method.name]),
                    "inType": "::".join(inputType),
                    "inVar": camelCase(inputType),
                    "outType": "::".join(outputType),
                    "outVar": camelCase(outputType),
                })
            serviceName = CamelCase([service.name])
            services.append({
                "name": serviceName,
                "class": f"I{serviceName}Service",
                "methods": methods
            })
        fileBase = os.path.splitext(os.path.split(pFile.name)[1])[0]
        return {
            "fileBase": fileBase,
            "headerGuard": "_{}_XCRPC_H_".format(fileBase.upper()),
            "services": services,
        }


class CppCppGenerator(CodeGenerator):
    def getTemplateFileName(self):
        return "Service.xcrpc.cpp.j2"

    def getOutputFileName(self, namespace, filebase):
        return os.path.join(namespace, f"{filebase}.xcrpc.cpp")

    def getFileInfo(self, pFile):
        services = []
        for service in pFile.service:
            serviceName = CamelCase([service.name])
            services.append({
                "class": f"I{serviceName}Service",
            })
        fileBase = os.path.splitext(os.path.split(pFile.name)[1])[0]
        return {
            "fileBase": fileBase,
            "services": services,
        }


class PyGenerator(CodeGenerator):
    def getTemplateFileName(self):
        return "Service_xcrpc.py.j2"

    def getOutputFileName(self, namespace, filebase):
        return f"{filebase}_xcrpc.py"

    def getFileInfo(self, pFile):
        services = []
        for service in pFile.service:
            methods = []
            for method in service.method:
                inputPackage, inputClass, inputType = _parseType(
                    method.input_type)
                outputPackage, outputClass, outputType = _parseType(
                    method.output_type)

                if not inputPackage:
                    raise ValueError(
                        "package not specified for type {}; please put a package statement in the proto file"
                        .format(inputType))
                if not outputPackage:
                    raise ValueError(
                        "package not specified for type {}; please put a package statement in the proto file"
                        .format(outputType))

                methods.append({
                    "name": CamelCase([method.name]),
                    "function": camelCase([method.name]),
                    "inPackage": pythonPackage(inputType),
                    "inClass": inputType[-1],
                    "inVar": camelCase(inputClass),
                    "outPackage": pythonPackage(outputType),
                    "outClass": outputType[-1],
                    "outVar": camelCase(outputClass),
                })
            services.append({
                "class": CamelCase(service.name),
                "methods": methods
            })
        return {
            "services": services,
        }


class JsGenerator(CodeGenerator):
    def getTemplateFileName(self):
        return "Service_xcrpc.js.j2"

    def getOutputFileName(self, namespace, filebase):
        return f"{filebase}_xcrpc.js"

    def getFileInfo(self, pFile):
        services = []
        packages = []
        for service in pFile.service:
            methods = []
            for method in service.method:
                inputPackage, inputClass, inputType = _parseType(
                    method.input_type)
                outputPackage, outputClass, outputType = _parseType(
                    method.output_type)

                if not inputPackage:
                    raise ValueError(
                        "package not specified for type {}; please put a package statement in the proto file"
                        .format(inputType))
                if not outputPackage:
                    raise ValueError(
                        "package not specified for type {}; please put a package statement in the proto file"
                        .format(outputType))

                outPack = self._parseIOPackage(outputPackage, outputClass)
                if outPack["name"] not in [p["name"] for p in packages]:
                    packages.append(outPack)

                inPack = self._parseIOPackage(inputPackage, inputClass)
                if inPack["name"] not in [p["name"] for p in packages]:
                    packages.append(inPack)

                methods.append({
                    "name": CamelCase([method.name]),
                    "function": camelCase([method.name]),
                    "inClass": CamelCase(inputType[-1]),
                    "inVar": camelCase(inputType[-1]),
                    "inTypeSig": ".".join(inputType),
                    "outPackageAlias": outPack["alias"],
                    "outClass": CamelCase(outputType[-1]),
                    "outVar": camelCase(outputType[-1]),
                })

            serviceName = CamelCase([service.name])
            services.append({
                "name": serviceName,
                "class": f"{serviceName}Service",
                "methods": methods
            })

        content = {
            "fileBase": os.path.splitext(os.path.split(pFile.name)[1])[0],
            "services": services,
            "packages": list(packages),
        }
        return content

    def _parseIOPackage(self, ioPackage, ioClass):
        if ioPackage[0] == "google":
            packageParts = ["google-protobuf"]
            packageParts.extend(list(ioPackage))
            packageParts.append("{}_pb".format(ioClass[0].lower()))
            ioPack = {
                "name": "/".join(packageParts),
                "alias": "proto_{}".format(ioClass[0].lower())
            }
        else:
            packageParts = ["."]
            packageParts.extend(list(ioPackage))
            packageParts[-1] = packageParts[-1] + "_pb"
            ioPack = {
                "name": "/".join(packageParts),
                "alias": ioPackage[-1][0].lower() + ioPackage[-1][1:]
            }
        return ioPack


class JsServiceInfoGenerator(CodeGenerator):
    def getTemplateFileName(self):
        return "Service_info.js.j2"

    def getOutputFileName(self, namespace, filebase):
        return f"{filebase}_serviceInfo.js"

    def getFileInfo(self, pFile):
        services = []
        packages = []
        for service in pFile.service:
            methods = []
            for method in service.method:
                inputPackage, inputClass, inputType = _parseType(
                    method.input_type)
                outputPackage, outputClass, outputType = _parseType(
                    method.output_type)
                methods.append({
                    "name": CamelCase([method.name]),
                    "function": camelCase([method.name]),
                    "inTypeSig": ".".join(inputType),
                    "outTypeSig": ".".join(outputType),
                })
            services.append({
                "name": CamelCase([service.name]),
                "methods": methods
            })

        content = {
            "fileBase": os.path.splitext(os.path.split(pFile.name)[1])[0],
            "services": services,
        }
        return content


# Map the plugin type to the template files to use
generatorClassesMap = {
    "protoc-gen-xcrpc_cpp": [CppCppGenerator, CppHGenerator],
    "protoc-gen-xcrpc_js": [JsGenerator],
    "protoc-gen-xcrpc-serviceinfos_js": [JsServiceInfoGenerator],
    "protoc-gen-xcrpc_py": [PyGenerator]
}


def _parseType(typeName):
    """parse a protobuf type into (package, class, full_type)"""
    components = typeName.lstrip(".").split(".")
    return (tuple(components[:-1]), tuple(components[-1:]), tuple(components))


def generate_code(request, response):
    generatorType = os.path.basename(__file__)
    generatorClasses = generatorClassesMap[generatorType]
    for pFile in request.proto_file:
        if "google" in pFile.name:
            continue
        namespace, pFilename = os.path.split(pFile.name)
        protoBase, _ = os.path.splitext(pFilename)

        for generatorClass in generatorClasses:
            generator = generatorClass()

            tmplName = generator.getTemplateFileName()

            filename = generator.getOutputFileName(namespace, protoBase)

            fileInfo = generator.getFileInfo(pFile)

            tmpl = jenv.get_template(tmplName)
            content = tmpl.render(**fileInfo)

            genFile = response.file.add()
            genFile.name = filename
            genFile.content = content


if __name__ == '__main__':
    # Read request message from stdin
    data = sys.stdin.buffer.read()

    # Parse request
    request = plugin.CodeGeneratorRequest()
    request.ParseFromString(data)

    # Create response
    response = plugin.CodeGeneratorResponse()

    # Generate code
    generate_code(request, response)

    # Serialise response message
    output = response.SerializeToString()

    # Write to stdout
    sys.stdout.buffer.write(output)
