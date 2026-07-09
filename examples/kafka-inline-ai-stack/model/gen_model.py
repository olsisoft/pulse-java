#!/usr/bin/env python3
"""Generate fraud-risk.onnx — a tiny, auditable transaction-fraud scorer.

  fraudScore = sigmoid(W · features + b)         # in [0,1]
features, IN THIS ORDER (the mlPredict operator packs them into [1,5]):
  [ amount_f , is_foreign , card_not_present , night , velocity_24h ]

The categorical risk signals (foreign card, card-not-present, night, velocity)
dominate; the raw amount only nudges, so a large but otherwise-normal purchase
isn't auto-flagged. Hand-set so behaviour is obvious:
  legit   (40,   0, 0, 0, 2)  -> fraud ~0.08
  at-risk (4200, 1, 1, 1, 9)  -> fraud ~0.98

Build:  pip install onnx numpy ; python3 gen_model.py
A prebuilt copy ships alongside, so Python isn't needed to run the demo.
"""
import os, numpy as np, onnx
from onnx import TensorProto, helper, numpy_helper

#               amount    is_foreign  card_not_present  night    velocity_24h
W = np.array([[ 0.00015 ,  1.4      ,   1.3            ,  1.1   ,  0.25       ]],
             dtype=np.float32).T            # [5,1]
B = np.array([-3.0], dtype=np.float32)
HERE = os.path.dirname(os.path.abspath(__file__)); OUT = os.path.join(HERE, "fraud-risk.onnx")

def build():
    x = helper.make_tensor_value_info("features", TensorProto.FLOAT, [1, 5])
    y = helper.make_tensor_value_info("fraudScore", TensorProto.FLOAT, [1, 1])
    g = helper.make_graph(
        [helper.make_node("MatMul", ["features", "W"], ["wx"]),
         helper.make_node("Add", ["wx", "B"], ["z"]),
         helper.make_node("Sigmoid", ["z"], ["fraudScore"])],
        "fraud-risk", [x], [y],
        [numpy_helper.from_array(W, "W"), numpy_helper.from_array(B, "B")])
    m = helper.make_model(g, producer_name="streamflow-pulse-example",
                          opset_imports=[helper.make_opsetid("", 13)])
    m.ir_version = 9
    onnx.checker.check_model(m); return m

def sig(x): return 1/(1+np.exp(-x))
if __name__ == "__main__":
    m = build(); onnx.save(m, OUT)
    print(f"wrote {OUT} ({os.path.getsize(OUT)} bytes)")
    for n,f in {"legit":[40,0,0,0,2],"borderline":[800,1,0,0,3],"fraud":[4200,1,1,1,9]}.items():
        z = float((np.array(f,dtype=np.float32)@W).ravel()[0]+B[0])
        print(f"  {n:10s} {f} -> fraudScore={sig(z):.4f}")
