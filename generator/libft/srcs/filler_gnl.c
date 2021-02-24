/* ************************************************************************** */
/*                                                                            */
/*                                                        :::      ::::::::   */
/*   filler_gnl.c                                       :+:      :+:    :+:   */
/*                                                    +:+ +:+         +:+     */
/*   By: sikpenou <marvin@42.fr>                    +#+  +:+       +#+        */
/*                                                +#+#+#+#+#+   +#+           */
/*   Created: 2019/10/08 14:04:12 by sikpenou          #+#    #+#             */
/*   Updated: 2019/10/29 10:58:16 by sikpenou         ###   ########.fr       */
/*                                                                            */
/* ************************************************************************** */

#include "libft.h"
#include <unistd.h>

int		read_loop(char buf[4000], int fd, int *pos, char **line)
{
	int		ret;

	ret = 4000;
	while (*pos == -1 && ret > 0)
	{
		ret = read(fd, buf, 4000);
		if (ret <= 0)
		{
			return (0);
		}
		buf[ret] = 0;
		if (ret && !(*line = ft_strjoin_free(line, &buf, LEFT)))
		{
			return (0);
		}
		*pos = ft_strchr_pos(*line, '\n');
	}
	return (1);
}

int		filler_gnl(int fd, char **line)
{
	static char		*keep = NULL;
	char			buf[4001];
	int				pos;

	if (!line)
		return (0);
	*line ? ft_free((void **)line) : 0;
	*line = NULL;
	if (keep && !(*line = ft_strdup(keep)))
		return (0);
	keep ? ft_free((void **)&keep) : 0;
	pos = *line ? ft_strchr_pos(*line, '\n') : -1;
	if (pos == -1 && !(read_loop(buf, fd, &pos, line)))
		return (0);
	if (pos > -1 && !(keep = ft_strdup(*line + pos + 1)))
		return (0);
	pos > -1 ? (*line)[pos + 1] = 0 : 0;
	return (*line ? 1 : 0);
}
