#!/usr/bin/env python3
"""Generate failure-risk.onnx — the predictive-maintenance scorer.

A deliberately tiny, fully-explainable model so the demo's predictions make
sense and reviewers can read the weights: a logistic scorer

    riskScore = sigmoid(W · features + b)          # output in [0, 1]

over four sensor features, IN THIS ORDER (the mlPredict operator packs the
pipeline's inputFields into the model's [1,4] input tensor in declared order):

    [ vibration_mm_s , bearing_temp_c , rpm , motor_current_a ]

The weights are hand-chosen (not trained) so the behaviour is obvious:
  * a healthy machine (vib≈2, temp≈55, current≈12)  → riskScore ≈ 0.05
  * a failing machine (vib≈11, temp≈95, current≈22) → riskScore ≈ 0.999

Only `onnx` is needed to BUILD the graph (no training stack):
    pip install onnx
    python3 gen_model.py        # writes failure-risk.onnx next to this file

The model is a stock ONNX graph (opset 13): MatMul → Add → Sigmoid, runnable by
any ONNX runtime — here the Pulse engine's embedded onnxruntime via mlPredict.
A prebuilt copy ships alongside, so you don't need Python to run the example.
"""
import os
import numpy as np
import onnx
from onnx import TensorProto, helper, numpy_helper

# Hand-set weights: vibration and bearing temp dominate, current contributes,
# rpm is near-neutral (steady-state machines run at a fixed rpm).
#                vibration  bearing_temp     rpm   motor_current
W = np.array([[   0.60   ,    0.12      , 0.0005,     0.20     ]],
             dtype=np.float32).T          # shape [4, 1]
B = np.array([-13.89], dtype=np.float32)  # bias → healthy≈0.05, failing≈0.999

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "failure-risk.onnx")


def build() -> onnx.ModelProto:
    features = helper.make_tensor_value_info("features", TensorProto.FLOAT, [1, 4])
    risk = helper.make_tensor_value_info("riskScore", TensorProto.FLOAT, [1, 1])

    w_init = numpy_helper.from_array(W, name="W")
    b_init = numpy_helper.from_array(B, name="B")

    matmul = helper.make_node("MatMul", ["features", "W"], ["wx"])
    add = helper.make_node("Add", ["wx", "B"], ["z"])
    sigmoid = helper.make_node("Sigmoid", ["z"], ["riskScore"])

    graph = helper.make_graph(
        [matmul, add, sigmoid], "failure-risk",
        [features], [risk], [w_init, b_init])
    model = helper.make_model(
        graph, producer_name="streamflow-pulse-example",
        opset_imports=[helper.make_opsetid("", 13)])
    model.ir_version = 9  # onnxruntime 1.20 accepts IR<=10; pin 9 for portability
    onnx.checker.check_model(model)
    return model


def sigmoid(x):
    return 1.0 / (1.0 + np.exp(-x))


def report(model):
    """Print a few predictions so the weights' behaviour is auditable."""
    cases = {
        "healthy":  [2.0, 55.0, 1500.0, 12.0],
        "warning":  [6.5, 74.0, 1500.0, 17.0],
        "failing":  [11.0, 95.0, 1500.0, 22.0],
    }
    print("sanity (computed from the same W,b baked into the graph):")
    for name, f in cases.items():
        z = float((np.array(f, dtype=np.float32) @ W).ravel()[0] + B[0])
        print(f"  {name:8s} {f} -> riskScore={sigmoid(z):.4f}")


if __name__ == "__main__":
    m = build()
    onnx.save(m, OUT)
    print(f"wrote {OUT} ({os.path.getsize(OUT)} bytes)")
    report(m)
