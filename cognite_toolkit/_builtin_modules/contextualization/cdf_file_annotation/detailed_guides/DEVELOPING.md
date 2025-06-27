# Developing and Extending the Annotation Toolkit

This guide is for developers who need to extend the functionality of the Annotation Toolkit beyond what is available in the `extraction_pipeline_config.yaml` file. It explains the toolkit's interface-based architecture and provides a step-by-step guide on how to create and integrate custom service implementations.

Before reading this, you should be familiar with the high-level architecture described in the main `README.md`.

## The Philosophy: Configuration vs. Code

The toolkit is built on a core philosophy of balancing ease of use with ultimate flexibility:

- **Configuration for Intent**: The `extraction_pipeline_config.yaml` is designed to handle the most common variations between projects (the "80%"). This includes pointing to different data models, defining data retrieval filters, setting thresholds, and toggling features. For most standard use cases, you should only need to edit this file.

- **Code for Complexity**: When a project's requirements are highly specialized (the "20%"), configuration is not enough. Scenarios like complex business logic, unique performance optimizations, or integration with external systems require custom code. This is where the toolkit's interface-based architecture becomes essential.

> **Rule of Thumb**: If you find yourself trying to express complex procedural logic (e.g., "if-this-then-that") in the YAML, it's a sign that you should probably write a custom Python implementation of a service interface instead.

## The Interface-Based Architecture

The toolkit is designed around a set of "service interfaces" (defined as Python Abstract Base Classes - ABCs). An interface is a contract that defines what a service should do by specifying its methods and their signatures.

The toolkit provides a `General...Service` class for most interfaces. This is the default implementation that reads from the `toolkit_config.yaml` and performs the standard logic. By creating your own class that inherits from the same interface, you can provide a custom implementation that the toolkit will use instead.

### Key Service Interfaces for Customization

While any service can be replaced, these are the most common candidates for custom implementations:

- **`AbstractLaunchService`**: The orchestrator for the launch function. You would implement this if your project requires a fundamentally different file batching, grouping, or processing workflow that can't be achieved with the `primary_scope_property` and `secondary_scope_property` configuration.

- **`IDataModelService`**: The gateway to Cognite Data Fusion. Implement this if your project needs highly optimized or complex queries to fetch files and entities that go beyond the declarative `QueryConfig` filter system.

- **`IApplyService`**: The service responsible for writing annotations back to the data model. Implement this if your project has custom rules for how to set annotation properties (like status) or needs to create additional relationships in the data model.

- **`ICacheService`**: Manages the in-memory entity cache. You might implement this if your project has a different caching strategy (e.g., different cache key logic, or fetching context from an external system).

## How to Create a Custom Implementation: A Step-by-Step Example

Let's walk through a common scenario: your project requires a unique way to organize files for processing that the default `GeneralLaunchService` doesn't support.

**Scenario**: We need to process files from a specific set of high-priority units first, regardless of their site, before processing any other files.

### Step 1: Create Your Custom Service Class

In your project's function code (e.g., inside the `services` directory), create a new Python file or add to an existing one. Define your new class, making sure it inherits from the correct abstract interface (`AbstractLaunchService` in this case).

```python
# In your project's services/my_custom_launch_service.py

from .LaunchService import AbstractLaunchService, GeneralLaunchService
from utils.DataStructures import FileProcessingBatch
from cognite.client.data_classes.data_modeling import NodeList

class HighPriorityLaunchService(GeneralLaunchService):
    """
    A custom launch service that prioritizes files from specific units.
    It inherits from GeneralLaunchService to reuse methods like _process_batch
    but overrides the main file organization logic.
    """
    def __init__(self, *args, **kwargs):
        # It's good practice to call the parent constructor to ensure
        # all base attributes (like config, client, logger) are set up.
        super().__init__(*args, **kwargs)

        # You could fetch custom config from your YAML here if needed
        self.high_priority_units = ["UNIT-001", "UNIT-007"]
        self.logger.info(f"Initialized HighPriorityLaunchService. Prioritizing units: {self.high_priority_units}")

    def _organize_files_for_processing(self, all_file_nodes: NodeList) -> list[FileProcessingBatch]:
        """
        Overrides the default organization logic.

        This custom implementation will create two main groups of batches:
        1. Batches for high-priority units.
        2. Batches for all other units.
        """
        self.logger.info("Using custom high-priority organization logic.")

        priority_files = []
        other_files = []

        # First, separate files into priority and non-priority lists
        for node in all_file_nodes:
            # Assuming 'sysUnit' is the property name for the unit
            unit = node.properties.get(self.file_view.as_view_id()).get("sysUnit")
            if unit in self.high_priority_units:
                priority_files.append(node)
            else:
                other_files.append(node)

        # Now, use the parent's organization logic on each list separately
        # This reuses the L1/L2 context grouping for the files within each priority group
        priority_batches = super()._organize_files_for_processing(NodeList(priority_files))
        other_batches = super()._organize_files_for_processing(NodeList(other_files))

        self.logger.info(f"Created {len(priority_batches)} high-priority batches and {len(other_batches)} standard batches.")

        # Return the combined list, with priority batches first
        return priority_batches + other_batches
```

**Note**: In this example, we inherit from `GeneralLaunchService` to reuse its `_organize_files_for_processing` method for the sub-lists. If your logic was completely different, you would inherit from `AbstractLaunchService` and implement the necessary methods from scratch.

### Step 2: Use Your Custom Implementation

```python
# In fn_dm_context_annotation_launch/handler.py

# ... (other imports)
from services.LaunchService import AbstractLaunchService
# 1. Import your new custom class
from services.my_custom_launch_service import HighPriorityLaunchService

# 2. Instantiate your new custom class instead of GeneralLaunchService
def _create_launch_service(config, client, logger, tracker) -> AbstractLaunchService:
    cache_instance: ICacheService = create_general_cache_service(config, client, logger)
    data_model_instance: IDataModelService = create_general_data_model_service(
        config, client, logger
    )
    annotation_instance: IAnnotationService = create_general_annotation_service(
        config, client, logger
    )
    launch_instance = HighPriorityLaunchService(
        client=client,
        config=config,
        logger=logger,
        tracker=tracker,
        data_model_service=data_model_instance,
        cache_service=cache_instance,
        annotation_service=annotation_instance,
    )
    return launch_instance

# ... (rest of the file)
```

When the function now runs, you'll have an instance of `HighPriorityLaunchService` wherever an `AbstractLaunchService` is required. The rest of the application (like the `handler.py`) continues to function as before, completely unaware of the specific implementation it's using. It only knows it's working with a service that fulfills the `AbstractLaunchService` contract.

This powerful pattern allows you to surgically replace or extend any part of the toolkit's logic to meet your project's specific needs while leveraging the stability and functionality of the surrounding framework.
