from typing import Callable, Dict, List, Optional
from pydantic import BaseModel, Field
import logging
import weakref

import matplotlib.pyplot as plt

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


class Pipe(BaseModel):
    value: Dict = Field(default_factory=dict)
    trace: bool = False
    functions: List[Callable] = Field(default_factory=list)

    class Config:
        arbitrary_types_allowed = True

    def __init__(self, **data):
        super().__init__(**data)
        if self.trace:
            self.value["trace"] = [{"function": "input", "output": self.value.copy()}]

    def __or__(self, func: Callable):
        if callable(func):
            self.functions.append(func)
            return self
        raise ValueError("Operand must be a callable")

    def __call__(self):
        for func in self.functions:
            try:
                result = func(**self.value)
                if isinstance(result, dict):
                    self.value.update(result)
                    if self.trace:
                        self.value["trace"].append({"function": func.__name__, "output": result.copy()})
                else:
                    raise ValueError(f"Function {func.__name__} must return a dictionary.")
            except Exception as e:
                logging.error(f"Error in pipeline execution at function {func.__name__}: {e}")
                return None
        return self.value.copy()  # Ensure immutability

    def __repr__(self):
        return f"Pipe({self.value!r})"

    def __getitem__(self, key=None):
        return self.value if key is None else self.value.get(key, None)


class Node(BaseModel):
    name: str
    data: Optional[dict] = None
    inputs: weakref.WeakSet = Field(default_factory=weakref.WeakSet)
    outputs: weakref.WeakSet = Field(default_factory=weakref.WeakSet)

    class Config:
        arbitrary_types_allowed = True

    def add_input(self, node: "Node"):
        if node not in self.inputs:
            self.inputs.add(node)
            node.add_output(self)

    def add_output(self, node: "Node"):
        if node not in self.outputs:
            self.outputs.add(node)
            node.add_input(self)

    def process(self):
        raise NotImplementedError("The process method should be implemented by subclasses")

    def __repr__(self):
        return f"Node(name={self.name}, data={self.data})"

    def __hash__(self):
        """Use the unique name to define hashability."""
        return hash(self.name)


class DataNode(Node):
    data_history: List[Optional[dict]] = Field(default_factory=list)

    def process(self):
        if self.data is not None:
            self.data_history.append(self.data.copy())
            logging.info(f"{self.name} processing data: {self.data}")
        for output in self.outputs:
            output.data = self.data.copy() if self.data else None
            output.process()

    def get_data_history(self):
        return self.data_history


class ProcessingNode(Node):
    pipeline: Optional[Pipe] = None

    def set_pipeline(self, pipeline: Pipe):
        self.pipeline = pipeline

    def process(self):
        if self.data is not None and self.pipeline is not None:
            # Update the pipeline's internal value dictionary
            self.pipeline.value.update(self.data)

            # Execute the pipeline
            result = self.pipeline()
            if result:
                self.data = result.copy()  # Store the result safely
                logging.info(f"{self.name} processed data: {self.data}")

            # Send processed data to connected nodes
        for output in self.outputs:
            output.data = self.data.copy() if self.data else None
            output.process()


class MixerNode(Node):
    def process(self):
        if len(self.inputs) >= 2:
            data1 = self.inputs[0].data
            data2 = self.inputs[1].data
            if data1 and data2:
                combined_data = self.custom_merge(data1, data2)
                self.data = combined_data.copy()
                logging.info(f"{self.name} combined data: {self.data}")
                for output in self.outputs:
                    output.data = self.data.copy()
                    output.process()
        else:
            logging.warning(f"{self.name} requires at least two inputs to combine data.")

    @staticmethod
    def custom_merge(dict1, dict2):
        merged = dict1.copy()
        for key, value in dict2.items():
            if key in merged:
                if isinstance(merged[key], list) and isinstance(value, list):
                    merged[key].extend(value)
                elif isinstance(merged[key], list):
                    merged[key].append(value)
                elif isinstance(value, list):
                    merged[key] = [merged[key]] + value
                else:
                    merged[key] = [merged[key], value]
            else:
                merged[key] = value
        return merged.copy()

class Layer:
    def __init__(self, nodes: List[Node]):
        self.nodes = nodes

    def process(self):
        for node in self.nodes:
            try:
                node.process()
            except Exception as e:
                logging.error(f"Error processing node {node.name}: {e}")

    def connect_to(self, next_layer: "Layer"):
        for node in self.nodes:
            for next_node in next_layer.nodes:
                node.add_output(next_node)

    def __repr__(self):
        return f"Layer({self.nodes})"

class Graph:
    """
    An object-oriented graph visualisation class that organizes nodes into layers.
    Calling visualise() displays the graph structure.
    """

    def __init__(self, layers: List[Layer]):
        self.layers = layers

    def visualise(self) -> None:
        # Compute positions: each layer is a column and nodes are evenly spaced vertically.
        positions = {}  # Map node -> (x, y)
        layer_count = len(self.layers)
        for layer_index, layer in enumerate(self.layers):
            num_nodes = len(layer.nodes)
            if num_nodes == 0:
                continue
            x = layer_index  # x coordinate is the layer index
            y_spacing = 1
            # Center nodes around y=0
            y_start = -((num_nodes - 1) / 2) * y_spacing
            for i, node in enumerate(layer.nodes):
                positions[node] = (x, y_start + i * y_spacing)

        # Create the plot
        fig, ax = plt.subplots(figsize=(8, 6))

        # Draw nodes
        for node, (x, y) in positions.items():
            circle = plt.Circle((x, y), 0.1, color='skyblue', zorder=3)
            ax.add_artist(circle)
            ax.text(x, y, node.name, ha='center', va='center', fontsize=9, zorder=4)

        # Draw connections as arrows
        for node, (x, y) in positions.items():
            for output in node.outputs:
                if output in positions:
                    x2, y2 = positions[output]
                    ax.annotate("",
                                xy=(x2, y2), xycoords='data',
                                xytext=(x, y), textcoords='data',
                                arrowprops=dict(arrowstyle="->", color='gray', lw=1),
                                zorder=2)

        ax.set_xlim(-1, layer_count)
        ys = [pos[1] for pos in positions.values()]
        if ys:
            ax.set_ylim(min(ys) - 1, max(ys) + 1)

        ax.set_aspect('equal')
        ax.axis('off')
        plt.title("Graph Visualisation")
        plt.show()