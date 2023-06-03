from __future__ import annotations
import doctest

import pathlib
import sys
import tempfile
from typing import Dict, Optional, Tuple

import np_logging
import np_session
import pandas as pd
import pynwb
from allensdk.brain_observatory.nwb import (
    read_eye_dlc_tracking_ellipses,
    read_eye_gaze_mappings,
    add_eye_tracking_ellipse_fit_data_to_nwbfile,
    add_eye_gaze_mapping_data_to_nwbfile,
    eye_tracking_data_is_valid,
)
from allensdk.brain_observatory.ecephys.nwb import (
    EcephysEyeTrackingRigMetadata,
)
import allensdk.brain_observatory.ecephys.write_nwb.__main__ as write_nwb

import np_eyetracking.dlc_lims.rig_geometry as rig_geometry
import np_eyetracking.dlc_lims.utils as utils
import np_eyetracking.dlc_lims.run_dlc as run_dlc


logger = np_logging.getLogger(__name__)


def add_to_nwb(
    session: str | pathlib.Path | np_session.Session,
    nwb: Optional[pynwb.NWBFile] = None,
) -> pynwb.NWBFile:
    
    session = np_session.Session(session)
    
    if nwb is None:
        logger.info('Creating new pynwb.NWBFile instance')
        nwbfile = pynwb.NWBFile(
            session_description='Data and metadata for an Ecephys session',
            identifier=f'{session}',
            session_id=f'{session}',
            session_start_time=session.start,
            institution='Allen Institute',
        )
    elif isinstance(nwb, pynwb.NWBFile):
        nwbfile = nwb
    else:
        raise TypeError(
            f'Expected type(nwb) == pynwb.NWBFIle, received: {type(nwb)}'
        )

    # Get necessary components
    eye_gaze_mapping_path = None    # TODO 
    eye_tracking_rig_geometry = rig_geometry.from_session(session).dict()
    try:
        eye_dlc_ellipses_path = utils.get_eye_tracking_paths(session)[
            'raw_eye_tracking_filepath'
        ]
    except FileNotFoundError:
        run_dlc.main(session)
        raise RuntimeError('DLC has been started - try adding to NWB again in a few hours')

    nwbfile = write_nwb.add_eye_tracking_rig_geometry_data_to_nwbfile(
        nwbfile, eye_tracking_rig_geometry
    )

    eye_tracking_frame_times = utils.get_sync_file_frame_times(session)
    eye_dlc_tracking_data = read_eye_dlc_tracking_ellipses(
        pathlib.Path(eye_dlc_ellipses_path)
    )

    num_sync_timestamps = len(eye_tracking_frame_times)
    num_eye_frames = len(eye_dlc_tracking_data['pupil_params'])

    if (num_sync_timestamps - num_eye_frames) < 5:
        # It's possible for there to be more timestamps than frames in a
        # case of non-transferred frames/aborted frames
        # See discussion in https://github.com/AllenInstitute/AllenSDK/issues/2376 # noqa
        # Truncate timestamps to match the number of frames
        logger.info(
            f'Number of eye tracking timestamps: {num_sync_timestamps}. '
            f'Number of eye tracking frames: {num_eye_frames}. '
            f'Truncating timestamps'
        )
        eye_tracking_frame_times = eye_tracking_frame_times[:num_eye_frames]
    elif num_eye_frames > num_sync_timestamps:
        raise AssertionError(
            f'Number of eye tracking timestamps: {num_sync_timestamps}. '
            f'Number of eye tracking frames: {num_eye_frames}. '
            f'We expect these to be equal'
        )

    if eye_gaze_mapping_path is not None:
        eye_gaze_data = read_eye_gaze_mappings(pathlib.Path(eye_gaze_mapping_path))
    else:
        eye_gaze_data = None

    nwbfile = write_nwb.add_eye_tracking_data_to_nwbfile(
        nwbfile, eye_tracking_frame_times, eye_dlc_tracking_data, eye_gaze_data
    )
    return nwbfile



def load_nwb_from_disk(
    nwb_path: str | pathlib.Path,
    ) -> pynwb.NWBFile:
    logger.info(f'Loading nwb file at {nwb_path}')
    with pynwb.NWBHDF5IO(nwb_path, mode='r') as f:
        return f.read()


def write_nwb_to_disk(
    nwb_file: pynwb.NWBFile, output_path: Optional[str | pathlib.Path] = None
    ) -> None:
    if output_path is None:
        output_path = pathlib.Path(tempfile.mkdtemp()) / f'{nwb_file.session_id}.nwb'
    
    nwb_file.set_modified()

    logger.info(f'Writing nwb file `{nwb_file.session_id!r}` to {output_path}')
    with pynwb.NWBHDF5IO(output_path, mode='w') as f:
        f.write(nwb_file, cache_spec=True)
    logger.debug(f'Writing complete for nwb file `{nwb_file.session_id!r}`')


def main(
    session_folder: str | pathlib.Path | np_session.Session,
    nwb_file: str | pathlib.Path | pynwb.NWBFile,
    output_file: Optional[str | pathlib.Path] = None,
) -> pynwb.NWBFile:
    
    session = np_session.Session(session_folder)
    
    if not isinstance(nwb_file, pynwb.NWBFile):
        nwb_file = load_nwb_from_disk(nwb_file)
    
    nwb_file = add_to_nwb(session, nwb_file)
    
    if output_file is not None:
        write_nwb_to_disk(nwb_file, output_file)
        
    return nwb_file


if __name__ == '__main__':
    np_logging.getLogger()
    doctest.testmod(raise_on_error=True)
    main(*sys.argv[1:])
