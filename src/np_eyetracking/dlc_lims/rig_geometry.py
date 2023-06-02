import pydantic
from typing import Dict, Tuple, Literal

import np_session


class RigGeometry(pydantic.BaseModel):
    """Rig geometry, requires following kwargs on init:

    equipment: A string describing rig
    monitor_position_mm: [x, y, z]
    monitor_rotation_deg: [x, y, z]
    camera_position_mm: [x, y, z]
    camera_rotation_deg: [x, y, z]
    led_position: [x, y, z]
    """

    equipment: str
    monitor_position_mm: Tuple[float, float, float]
    monitor_rotation_deg: Tuple[float, float, float]
    camera_position_mm: Tuple[float, float, float]
    camera_rotation_deg: Tuple[float, float, float]
    led_position: Tuple[float, float, float]


def from_session(session) -> RigGeometry:
    session = np_session.Session(str(session))

    config: Dict[str, Dict[str, float]] = session.rig.config['hardware']

    def to_xyz(
        key: str,
        position_or_rotation: Literal['p', 'r', 'position', 'rotation'],
    ) -> Tuple[float, float, float]:
        p_or_r = position_or_rotation.lower()
        if p_or_r.startswith('p'):
            sub_key = 'center_{}_mm'
        elif p_or_r.startswith('r'):
            sub_key = 'rotation_{}_deg'
        else:
            raise ValueError(
                f'`position_or_rotation` should start with `p` or `r`, received {p_or_r}'
            )
        return tuple(
            config[key][sub_key.format(dim)] for dim in ('x', 'y', 'z')
        )

    return RigGeometry(
        equipment=str(session.rig),
        monitor_position_mm=to_xyz('screen_position', 'p'),
        monitor_rotation_deg=to_xyz('screen_position', 'r'),
        camera_position_mm=to_xyz('eye_camera_position', 'p'),
        camera_rotation_deg=to_xyz('eye_camera_position', 'r'),
        led_position=to_xyz('eye_led_position', 'p'),
    )
