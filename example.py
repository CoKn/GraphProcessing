from Graph.graph import Pipe, DataNode, ProcessingNode, Layer, Graph, MixerNode
import logging


def add_value(value=1, **data):
    data["passthrough"] = data.get("passthrough", 0) + value
    return data


def multiply_value(**data):
    data["passthrough"] = data.get("passthrough", 1) * data.get("m", 1)
    return data


def subtract_value(value=3, **data):
    data["passthrough"] = data.get("passthrough", 0) - value
    return data


def square_value(**data):
    val = data.get("passthrough", 0)
    data["passthrough"] = val * val
    return data


if __name__ == '__main__':
    # Build the graph:
    # Layer 1: Input node
    input_node = DataNode(name="InputNode", data={"passthrough": 10})
    input_layer = Layer(nodes=[input_node])

    # Layer 2: Processing nodes (Adder and Multiplier)
    adder_node = ProcessingNode(name="AdderNode")
    multiplier_node = ProcessingNode(name="MultiplierNode")
    # Create pipelines with custom parameters
    adder_pipeline = Pipe() | (lambda **data: add_value(value=5, **data))
    multiplier_pipeline = Pipe(value={"m": 3}) | multiply_value
    adder_node.set_pipeline(adder_pipeline)
    multiplier_node.set_pipeline(multiplier_pipeline)
    processing_layer1 = Layer(nodes=[adder_node, multiplier_node])

    # Connect Layer 1 to Layer 2
    input_layer.connect_to(processing_layer1)

    # Layer 3: Further processing (Subtractor and Squarer)
    subtractor_node = ProcessingNode(name="SubtractorNode")
    squarer_node = ProcessingNode(name="SquarerNode")
    subtractor_pipeline = Pipe() | (lambda **data: subtract_value(value=3, **data))
    squarer_pipeline = Pipe() | square_value
    subtractor_node.set_pipeline(subtractor_pipeline)
    squarer_node.set_pipeline(squarer_pipeline)
    processing_layer2 = Layer(nodes=[subtractor_node, squarer_node])

    # Connect Layer 2 to Layer 3
    processing_layer1.connect_to(processing_layer2)

    # Layer 4: Mixer node to combine outputs from Layer 3
    mixer_node = MixerNode(name="MixerNode")
    mixer_layer = Layer(nodes=[mixer_node])

    # Connect Layer 3 to Mixer Layer
    processing_layer2.connect_to(mixer_layer)

    # Layer 5: Final output node
    output_node = DataNode(name="OutputNode")
    output_layer = Layer(nodes=[output_node])

    # Connect Mixer Layer to Output Layer
    mixer_layer.connect_to(output_layer)

    # Process the pipeline starting from the input layer
    input_layer.process()

    # Log data histories for demonstration
    logging.info(f"Data history in InputNode: {input_node.get_data_history()}")
    logging.info(f"Data history in OutputNode: {output_node.get_data_history()}")

    # Create a Graph instance and visualise the complete structure
    graph = Graph(layers=[input_layer, processing_layer1, processing_layer2, mixer_layer, output_layer])
    graph.visualise()