"""
Integration test for scurry_project_names() using Redshift backend.
"""
import os
from zombie_squirrel import unique_project_names


def main():
    # Set environment variable to use Redshift backend
    os.environ["TREE_SPECIES"] = "redshift"
    # Run the function
    project_names = unique_project_names(force_update=True)
    print("Project names:", project_names)
    assert isinstance(project_names, list), "Result should be a list"
    print("Integration test passed.")


if __name__ == "__main__":
    main()
