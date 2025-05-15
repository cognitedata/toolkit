# Three sixty reading

Set up capabilities for thressixty.

| Capability type | Action                              | Scope          | Description                                                            |
|-----------------|-------------------------------------|----------------|------------------------------------------------------------------------|
| Assets          | `assets:read`                       | Data sets, All | Find asset tags of equipment the robot works with and view asset data. |
| Events          | `events:read`, `events:write`       | Data sets, All | View events in the canvas.                                             |
| Files           | `files:read`, `files:write`         | Data sets, All | Allow users to upload images.                                          |
| Projects        | `projects:read`, `projects:list`    | Data sets, All | Extract the projects the user has access to.                           |
| Labels          | `label:read`, `label:write`         | Data sets, All | Extract the projects the user has access to.                           |
| Robotics        | `robotics:read`                     | Data sets, All | Get 3D alignment of robot.                                             |
| Functions       | `functions:write`, `functions:read` | Data sets, All | Create, call and schedule functions.                                   |
| Sessions        | `sessions:create`                   | Data sets, All | Call and schedule functions.                                           |
