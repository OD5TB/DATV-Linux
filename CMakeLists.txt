cmake_minimum_required(VERSION 3.16)
project(dvbs2_tx LANGUAGES CXX)

include(GNUInstallDirs)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(PkgConfig REQUIRED)
pkg_check_modules(HACKRF IMPORTED_TARGET libhackrf)
pkg_check_modules(LIBIIO IMPORTED_TARGET libiio)
pkg_check_modules(SOAPYSDR IMPORTED_TARGET SoapySDR)
find_package(Threads REQUIRED)

add_executable(dvbs2_tx
    main.cpp
    dvbs2_fec.cpp
    fec_selector.cpp
    optimization.cpp
    pl_header.cpp
)

target_include_directories(dvbs2_tx PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_compile_options(dvbs2_tx PRIVATE -O3 -Wall -Wextra -Wpedantic)
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    target_compile_options(dvbs2_tx PRIVATE -Wno-psabi)
endif()
target_link_libraries(dvbs2_tx PRIVATE Threads::Threads)

if(HACKRF_FOUND)
    message(STATUS "libhackrf found: enabling HackRF support")
    target_compile_definitions(dvbs2_tx PRIVATE HAVE_HACKRF=1)
    target_link_libraries(dvbs2_tx PRIVATE PkgConfig::HACKRF)
else()
    message(WARNING "libhackrf not found: HackRF support disabled")
endif()

if(LIBIIO_FOUND)
    message(STATUS "libiio found: enabling Pluto / Pluto+ / F5OEO support")
    target_compile_definitions(dvbs2_tx PRIVATE HAVE_PLUTO=1)
    target_link_libraries(dvbs2_tx PRIVATE PkgConfig::LIBIIO)
else()
    message(WARNING "libiio not found: Pluto / Pluto+ / F5OEO support disabled")
endif()

if(SOAPYSDR_FOUND)
    message(STATUS "SoapySDR found: enabling LimeSDR Mini support")
    target_compile_definitions(dvbs2_tx PRIVATE HAVE_SOAPYSDR=1)
    target_link_libraries(dvbs2_tx PRIVATE PkgConfig::SOAPYSDR)
else()
    message(WARNING "SoapySDR not found: LimeSDR Mini support disabled")
endif()

install(TARGETS dvbs2_tx RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
install(PROGRAMS
    dvbs2_gui.py
    check_dependencies.py
    install.sh
    DESTINATION ${CMAKE_INSTALL_BINDIR}
)
install(FILES
    LICENSE
    README.md
    UPGRADE_NOTES.md
    DATV-Linux_User_Manual.pdf
    requirements.txt
    test_pattern_datv.mp4
    DESTINATION ${CMAKE_INSTALL_DATADIR}/dvbs2_tx
)
