import os
import matplotlib.pyplot as plt

def test_heatmap_file_exists():
    assert os.path.exists("total_installed_power.png")

def test_density_map_file_exists():
    assert os.path.exists("charging_station_density.png")

def test_matplotlib_figure():
    fig, ax = plt.subplots()
    ax.plot([0, 1], [0, 1])
    assert fig is not None and ax is not None
