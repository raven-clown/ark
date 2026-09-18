import React from 'react';
import Header from '../../Header';
import Table from '../../../components/Table';
import Root from '../../../components/Root';
import { toast } from 'react-toastify';
import 'react-toastify/dist/ReactToastify.css';
import { withRouter } from '../../../utils/withRouter';
import { uriMyProjects, uriProjects } from '../../../utils/endpoints';

class ProjectList extends Root {
  state = {
    data: [],
    loading: true,
    showCreate: false,
    form: { name: '', slug: '', description: '' }
  };

  componentDidMount() {
    this.load();
  }

  async load() {
    this.setState({ loading: true });
    try {
      const res = await this.getApi(uriMyProjects());
      this.setState({
        data: (res.data || []).map(p => ({
          id: p.slug,
          name: p.name,
          slug: p.slug,
          role: p.callerRole || '-',
          members: (p.members || []).length,
          clusters: (p.clusters || []).join(', ') || '-'
        })),
        loading: false
      });
    } catch (err) {
      this.setState({ loading: false });
    }
  }

  handleFormChange = e => {
    const { name, value } = e.target;
    this.setState({ form: { ...this.state.form, [name]: value } });
  };

  create = async e => {
    e.preventDefault();
    const { form } = this.state;
    if (!form.name || !form.slug) {
      toast.error('Name and slug are required');
      return;
    }
    try {
      await this.postApi(uriProjects(), form);
      toast.success('Project created');
      this.setState({ showCreate: false, form: { name: '', slug: '', description: '' } });
      this.load();
    } catch (err) {
      // toasted by the api layer
    }
  };

  render() {
    const { data, loading, showCreate, form } = this.state;

    return (
      <div>
        <Header title="Projects">
          <button
            className="btn btn-primary ms-2"
            onClick={() => this.setState({ showCreate: !showCreate })}
          >
            {showCreate ? 'Close' : 'Create project'}
          </button>
        </Header>

        {showCreate && (
          <form className="khq-data-filter khq-nav p-3 mb-3" onSubmit={this.create}>
            <div className="row g-2 align-items-end">
              <div className="col-auto">
                <label className="form-label">Name</label>
                <input
                  className="form-control"
                  name="name"
                  value={form.name}
                  onChange={this.handleFormChange}
                />
              </div>
              <div className="col-auto">
                <label className="form-label">Slug</label>
                <input
                  className="form-control"
                  name="slug"
                  placeholder="lowercase, digits, - only"
                  value={form.slug}
                  onChange={this.handleFormChange}
                />
              </div>
              <div className="col-auto">
                <label className="form-label">Description</label>
                <input
                  className="form-control"
                  name="description"
                  value={form.description}
                  onChange={this.handleFormChange}
                />
              </div>
              <div className="col-auto">
                <button type="submit" className="btn btn-primary">
                  Create
                </button>
              </div>
            </div>
            <p className="mt-2 mb-0 text-muted">You become the project owner.</p>
          </form>
        )}

        <Table
          loading={loading}
          columns={[
            { id: 'name', accessor: 'name', colName: 'Name', sortable: true },
            { id: 'role', accessor: 'role', colName: 'Your role' },
            { id: 'members', accessor: 'members', colName: 'Members' },
            { id: 'clusters', accessor: 'clusters', colName: 'Clusters' }
          ]}
          actions={['details']}
          data={data}
          updateData={updated => this.setState({ data: updated })}
          noContent="You are not a member of any project yet - create one above."
          detailsHref={slug => `/ui/projects/${slug}`}
        />
      </div>
    );
  }
}

export default withRouter(ProjectList);
