import React from 'react';
import Header from '../../Header';
import Root from '../../../components/Root';
import ConfirmModal from '../../../components/Modal/ConfirmModal';
import { toast } from 'react-toastify';
import 'react-toastify/dist/ReactToastify.css';
import { withRouter } from '../../../utils/withRouter';
import {
  uriProject,
  uriProjectMembers,
  uriProjectMember,
  uriProjectClusters,
  uriProjectCluster
} from '../../../utils/endpoints';

const ROLES = ['VIEWER', 'DEVELOPER', 'MAINTAINER', 'OWNER'];
const MANAGE_ROLES = ['MAINTAINER', 'OWNER'];

class ProjectDetail extends Root {
  state = {
    project: null,
    loading: true,
    memberForm: { employeeCode: '', projectRole: 'DEVELOPER' },
    clusterForm: { clusterName: '' },
    confirm: null
  };

  componentDidMount() {
    this.load();
  }

  slug() {
    return this.props.params.projectSlug;
  }

  async load() {
    this.setState({ loading: true });
    try {
      const res = await this.getApi(uriProject(this.slug()));
      this.setState({ project: res.data, loading: false });
    } catch (err) {
      this.setState({ loading: false });
    }
  }

  canManage() {
    const { project } = this.state;
    return project && MANAGE_ROLES.includes(project.callerRole);
  }

  isOwner() {
    return this.state.project && this.state.project.callerRole === 'OWNER';
  }

  addMember = async e => {
    e.preventDefault();
    const { memberForm } = this.state;
    if (!memberForm.employeeCode) {
      toast.error('Employee code is required');
      return;
    }
    try {
      await this.postApi(uriProjectMembers(this.slug()), memberForm);
      toast.success('Member added');
      this.setState({ memberForm: { employeeCode: '', projectRole: 'DEVELOPER' } });
      this.load();
    } catch (err) {
      // toasted by the api layer
    }
  };

  changeRole = async (employeeCode, projectRole) => {
    try {
      await this.patchApi(uriProjectMember(this.slug(), employeeCode), { projectRole });
      toast.success('Role updated');
      this.load();
    } catch (err) {
      // toasted by the api layer
    }
  };

  removeMember = employeeCode => {
    this.setState({
      confirm: {
        message: `Remove ${employeeCode} from this project?`,
        onConfirm: async () => {
          this.setState({ confirm: null });
          try {
            await this.removeApi(uriProjectMember(this.slug(), employeeCode));
            toast.success('Member removed');
            this.load();
          } catch (err) {
            // toasted by the api layer
          }
        }
      }
    });
  };

  addCluster = async e => {
    e.preventDefault();
    const { clusterForm } = this.state;
    if (!clusterForm.clusterName) {
      toast.error('Cluster name is required');
      return;
    }
    try {
      await this.postApi(uriProjectClusters(this.slug()), clusterForm);
      toast.success('Cluster linked');
      this.setState({ clusterForm: { clusterName: '' } });
      this.load();
    } catch (err) {
      // toasted by the api layer
    }
  };

  removeCluster = clusterName => {
    this.setState({
      confirm: {
        message: `Unlink cluster ${clusterName} from this project? Members lose access to it.`,
        onConfirm: async () => {
          this.setState({ confirm: null });
          try {
            await this.removeApi(uriProjectCluster(this.slug(), clusterName));
            toast.success('Cluster unlinked');
            this.load();
          } catch (err) {
            // toasted by the api layer
          }
        }
      }
    });
  };

  render() {
    const { project, loading, memberForm, clusterForm, confirm } = this.state;

    if (loading || !project) {
      return (
        <div>
          <Header title="Project" />
        </div>
      );
    }

    const canManage = this.canManage();

    return (
      <div>
        <Header title={`Project - ${project.name}`} />

        <div className="khq-data-filter khq-nav p-3 mb-3">
          <p className="mb-1">{project.description || 'No description.'}</p>
          <p className="mb-0 text-muted">
            Slug: <code>{project.slug}</code> &middot; Your role:{' '}
            <b>{project.callerRole || 'none'}</b>
          </p>
        </div>

        <h3>Members</h3>
        <div className="khq-data-filter khq-nav p-3 mb-3">
          <table className="table">
            <thead>
              <tr>
                <th>Employee code</th>
                <th>Name</th>
                <th>Role</th>
                {canManage && <th>Actions</th>}
              </tr>
            </thead>
            <tbody>
              {(project.members || []).map(m => (
                <tr key={m.employeeCode}>
                  <td>{m.employeeCode}</td>
                  <td>{m.fullName}</td>
                  <td>
                    {this.isOwner() ? (
                      <select
                        className="form-select form-select-sm"
                        value={m.projectRole}
                        onChange={e => this.changeRole(m.employeeCode, e.target.value)}
                      >
                        {ROLES.map(r => (
                          <option key={r} value={r}>
                            {r}
                          </option>
                        ))}
                      </select>
                    ) : (
                      m.projectRole
                    )}
                  </td>
                  {canManage && (
                    <td>
                      <button
                        className="btn btn-danger btn-sm"
                        onClick={() => this.removeMember(m.employeeCode)}
                      >
                        Remove
                      </button>
                    </td>
                  )}
                </tr>
              ))}
            </tbody>
          </table>

          {canManage && (
            <form className="row g-2 align-items-end mt-2" onSubmit={this.addMember}>
              <div className="col-auto">
                <label className="form-label">Employee code</label>
                <input
                  className="form-control"
                  value={memberForm.employeeCode}
                  onChange={e =>
                    this.setState({ memberForm: { ...memberForm, employeeCode: e.target.value } })
                  }
                />
              </div>
              <div className="col-auto">
                <label className="form-label">Role</label>
                <select
                  className="form-select"
                  value={memberForm.projectRole}
                  onChange={e =>
                    this.setState({ memberForm: { ...memberForm, projectRole: e.target.value } })
                  }
                >
                  {ROLES.map(r => (
                    <option key={r} value={r}>
                      {r}
                    </option>
                  ))}
                </select>
              </div>
              <div className="col-auto">
                <button type="submit" className="btn btn-primary">
                  Add member
                </button>
              </div>
            </form>
          )}
        </div>

        <h3>Clusters</h3>
        <div className="khq-data-filter khq-nav p-3 mb-3">
          {(project.clusters || []).length === 0 && <p>No cluster linked yet.</p>}
          {(project.clusters || []).map(c => (
            <span key={c} className="badge bg-primary me-2 mb-2">
              {c}{' '}
              {canManage && (
                <span style={{ cursor: 'pointer' }} onClick={() => this.removeCluster(c)}>
                  ×
                </span>
              )}
            </span>
          ))}

          {canManage && (
            <form className="row g-2 align-items-end mt-2" onSubmit={this.addCluster}>
              <div className="col-auto">
                <label className="form-label">Cluster name</label>
                <input
                  className="form-control"
                  placeholder="must match a registered cluster connection"
                  value={clusterForm.clusterName}
                  onChange={e => this.setState({ clusterForm: { clusterName: e.target.value } })}
                />
              </div>
              <div className="col-auto">
                <button type="submit" className="btn btn-primary">
                  Link cluster
                </button>
              </div>
            </form>
          )}
        </div>

        <ConfirmModal
          show={!!confirm}
          message={confirm ? confirm.message : ''}
          handleConfirm={() => confirm && confirm.onConfirm()}
          handleCancel={() => this.setState({ confirm: null })}
        />
      </div>
    );
  }
}

export default withRouter(ProjectDetail);
