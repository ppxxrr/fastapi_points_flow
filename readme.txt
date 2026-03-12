README / 运维说明

项目结构

app/

data/exports/

deploy.sh

rollback.sh

本地开发
uvicorn app.main:app --reload
服务器发布
cd /opt/fastapi_points_flow
./deploy.sh
查看日志
journalctl -u myfastapi -n 50 --no-pager
journalctl -u myfastapi -f
回滚
git log --oneline -n 5
./rollback.sh <commit_id>
健康检查
curl http://127.0.0.1:8000/health
curl http://127.0.0.1/health