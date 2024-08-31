import pkg_resources
import subprocess


class PackageManager:

    def __init__(self):
        pass

    def identify_non_crucial_packages(self):
        """
        Identifies packages installed in the environment that are not
        crucial to keep. The list of crucial packages is predefined.

        Returns:
            set: A set of package names that are not crucial.
        """

        crucial_packages = {"pip", "setuptools", "wheel"}
        installed_packages = {pkg.key for pkg in pkg_resources.working_set}
        return installed_packages - crucial_packages

    def uninstall_packages(self, packages_to_uninstall):
        """
        Uninstalls the specified packages using pip.

        Args:
            packages_to_uninstall (set): A set of package names to be uninstalled.
        """

        for package in packages_to_uninstall:
            print(f"Uninstalling {package}...")
            subprocess.run(["pip", "uninstall", "-y", package])

    def install_packages_via_requirements(self, path_to_requirements):
        """
        Installs packages listed in a requirements file using pip.

        Args:
            path_to_requirements (str): Path to the requirements file.
        """

        subprocess.run(["pip", "install", "-r", path_to_requirements])

    def main(self):
        print("UNINSTALLING PACKAGES\n\n")
        packages_to_uninstall = self.identify_non_crucial_packages()
        self.uninstall_packages(packages_to_uninstall)
        print("\n\nUNINSTALL COMPLETE")

        requirements_files = ["../requirements.txt", "../requirements-dev.txt"]
        for path in requirements_files:
            print(f"\n\nINSTALL PACKAGES IN {path}\n\n")
            self.install_packages_via_requirements(path)
            print("\nINSTALL COMPLETE")


if __name__ == "__main__":
    pm = PackageManager()
    pm.main()
