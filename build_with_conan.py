#!/usr/bin/env python3
import os
import shutil
import argparse


def clean_build_folder(build_folder: str):
    if os.path.exists(build_folder):
        shutil.rmtree(build_folder)


def main(args):
    build_folder = "build"

    if args.clean:
        print("----------------------------------")
        print("cleaning build directory...")
        print("----------------------------------")
        clean_build_folder(build_folder)

    os.makedirs(build_folder, exist_ok=True)

    cmake_options = []
    conan_options = []
    if args.build_without_clang_tidy:
        cmake_options.append("-D BUILD_WITH_CLANG_TIDY=OFF")

    if args.release:
        cmake_options.append("-D CMAKE_BUILD_TYPE=Release")
    else:
        cmake_options.append("-D CMAKE_BUILD_TYPE=Debug")
        conan_options.append("-s build_type=Debug")

    print("----------------------------------")
    print(
        "conan install . --build=missing --profile ./conanprofile --profile:build ./conanprofile --output-folder=build "
        + " ".join(conan_options))
    print("----------------------------------")

    os.system(
        "conan install . --build=missing --profile ./conanprofile --profile:build ./conanprofile --output-folder=build "
        + " ".join(conan_options))

    print("----------------------------------")
    print("cmake " + " ".join(cmake_options) + " -B build")
    print("----------------------------------")

    os.system("cmake " + " ".join(cmake_options) + " -B build")

    print("----------------------------------")
    print(f"cmake --build build --parallel {args.parallel}")
    print("----------------------------------")

    os.system(f"cmake --build build --parallel {args.parallel}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--clean", action="store_true", help="Clean build directory before building")
    parser.add_argument("--release", action="store_true", help="Trigger RELEASE build")
    parser.add_argument("--build_without_clang_tidy", action="store_true", help="Build without clang-tidy")
    parser.add_argument("--parallel", type=int, default=1, help="Number of parallel jobs")

    args_parsed = parser.parse_args()
    main(args_parsed)