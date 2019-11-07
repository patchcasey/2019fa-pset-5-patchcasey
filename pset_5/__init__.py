from pkg_resources import get_distribution, DistributionNotFound

# adapted from Professor Gorlin's slide

try:
    from setuptools_scm import get_version

    __version__ = get_version(root="..", relative_to=__file__)
except:
    # package is not installed
    from setuptools_scm import get_version

    __version__ = get_version(root="..", relative_to=__file__)
